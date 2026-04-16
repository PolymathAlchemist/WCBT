"""
SQLite implementation of ProfileStore.

This module owns the on-disk persistence format for per-job authoring rules.

Notes
-----
Template is the authoritative owner of selection rules and compression policy.

Threading
---------
sqlite3 connections are not shared across threads in this implementation. If the
GUI uses this store, it should create and use the store entirely within the GUI
worker thread.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence, cast

from backup_engine.job_binding import JobBinding
from backup_engine.scheduling.models import BackupScheduleSpec, normalize_schedule_spec
from backup_engine.template_policy import TemplateSelectionRules

from ..paths_and_safety import ProfilePaths, ensure_profile_directories, resolve_profile_paths
from .api import JobId, JobSummary, ProfileStore
from .errors import IncompatibleProfileStoreError, InvalidRuleError, UnknownJobError
from .rules import normalize_template_selection_rules
from .schema import SCHEMA_V1

# Stable UUID namespace for JobId derivation.
# Changing this will invalidate all persisted job identities.
JOB_ID_NAMESPACE: Final[uuid.UUID] = uuid.UUID("6b0e2c7a-8e8f-4c5a-bb5d-9e6c6a3e7a01")


def _build_incompatible_contract_message(*, job_id: JobId, missing_field: str) -> str:
    """
    Return a user-facing message for a pre-contract profile database.

    Parameters
    ----------
    job_id:
        Identifier of the incompatible job row.
    missing_field:
        Required authoritative field that is missing from the persisted row.

    Returns
    -------
    str
        User-facing incompatibility message with recovery guidance.
    """

    return (
        "This profile database was created by a pre-contract WCBT build and is no longer "
        "compatible with the current refactored job/template schema. "
        f"The persisted job row is missing required field '{missing_field}' "
        f"(job_id: {job_id}). "
        "Delete and recreate the local profile database, or run a future migration tool if "
        "one is added later."
    )


def _new_template_id() -> str:
    """
    Return a new stable Template identifier.

    Returns
    -------
    str
        Newly generated Template identifier.
    """

    return uuid.uuid4().hex


def profile_store_db_path(profile_name: str, data_root: Path | None) -> Path:
    """
    Return the canonical path for the ProfileStore SQLite database.

    Parameters
    ----------
    profile_name:
        Name of the WCBT profile.
    data_root:
        Optional override for the WCBT data root.

    Returns
    -------
    pathlib.Path
        Path to the SQLite database under ProfilePaths.index_root.
    """
    paths: ProfilePaths = resolve_profile_paths(profile_name=profile_name, data_root=data_root)
    return paths.index_root / "profiles.sqlite"


def _normalize_template_compression(value: str) -> str:
    """
    Normalize Template-owned compression policy values.

    Parameters
    ----------
    value:
        Raw compression value from a compatibility surface.

    Returns
    -------
    str
        Normalized compression policy value.

    Raises
    ------
    InvalidRuleError
        If the value falls outside the current Template policy surface.
    """

    normalized = str(value).strip().lower()
    if normalized not in {"none", "zip", "tar.zst"}:
        raise InvalidRuleError("compression must be one of: none, zip, tar.zst.")
    return normalized


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Ensure the DB schema supports current ProfileStore features.

    Notes
    -----
    SQLite lacks `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so we inspect table
    info and apply minimal migrations as needed.
    """
    # Ensure profile_meta exists (for profile_id persistence).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS profile_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )

    # Ensure the authoritative Template policy tables and schedule table exist.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS template_selection_rules ("
        "template_id TEXT NOT NULL, "
        "kind TEXT NOT NULL CHECK(kind IN ('include','exclude')), "
        "pattern TEXT NOT NULL, "
        "position INTEGER NOT NULL, "
        "PRIMARY KEY (template_id, kind, position))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_template_selection_rules_kind "
        "ON template_selection_rules(template_id, kind)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS template_backup_policy ("
        "template_id TEXT PRIMARY KEY, "
        "compression TEXT NOT NULL DEFAULT 'none')"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS job_schedules ("
        "job_id TEXT PRIMARY KEY, "
        "cadence TEXT NOT NULL CHECK(cadence IN ('daily','weekly')), "
        "start_time_local TEXT NOT NULL, "
        "weekdays TEXT NULL, "
        "FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE)"
    )
    # Ensure jobs has is_deleted column for soft deletion.
    cols = conn.execute("PRAGMA table_info(jobs)").fetchall()
    col_names = {str(r["name"]) for r in cols}
    if "is_deleted" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")
    if "template_id" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN template_id TEXT NULL")
    if "source_root" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN source_root TEXT NULL")

    # Optional per-job UI state (restore tab).
    if "archive_root" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN archive_root TEXT NULL")
    if "restore_dest_root" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN restore_dest_root TEXT NULL")

    conn.execute("DROP TABLE IF EXISTS rules")
    conn.execute("DROP TABLE IF EXISTS job_backup_defaults")
    conn.execute("DROP TABLE IF EXISTS scheduled_backup_legacy_inputs")


def _get_or_create_profile_id(conn: sqlite3.Connection) -> str:
    """
    Return the persisted profile_id, creating it if missing.

    The profile_id is stored inside the profile store DB so that a profile can
    be moved/copied without changing job identity derivation.
    """
    row = conn.execute("SELECT value FROM profile_meta WHERE key = 'profile_id'").fetchone()
    if row is not None:
        return str(row["value"])

    profile_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO profile_meta(key, value) VALUES('profile_id', ?)",
        (profile_id,),
    )
    return profile_id


def _load_template_id(conn: sqlite3.Connection, job_id: JobId) -> str:
    """
    Return the authoritative Template identifier referenced by a job.

    Parameters
    ----------
    conn:
        Open SQLite connection.
    job_id:
        Identifier of the job whose Template reference should be loaded.

    Returns
    -------
    str
        Persisted Template identifier referenced by the job.

    Raises
    ------
    UnknownJobError
        If the job does not exist or has been soft deleted.
    IncompatibleProfileStoreError
        If the job row predates the post-cutover contract.
    """

    row = _load_existing_job_row(conn, job_id)
    template_id_raw = row["template_id"]
    if template_id_raw is None or not str(template_id_raw).strip():
        raise IncompatibleProfileStoreError(
            _build_incompatible_contract_message(job_id=job_id, missing_field="template_id")
        )
    return str(template_id_raw)


def _load_existing_job_row(conn: sqlite3.Connection, job_id: JobId) -> sqlite3.Row:
    """
    Return the active jobs row for an existing job.

    Parameters
    ----------
    conn:
        Open SQLite connection.
    job_id:
        Identifier of the job to load.

    Returns
    -------
    sqlite3.Row
        Active jobs row for the requested job.

    Raises
    ------
    UnknownJobError
        If the job does not exist or has been soft deleted.
    """

    row = conn.execute(
        "SELECT job_id, name, template_id, source_root FROM jobs "
        "WHERE job_id = ? AND is_deleted = 0",
        (job_id,),
    ).fetchone()
    if row is None:
        raise UnknownJobError(f"Unknown job_id: {job_id}")
    return cast(sqlite3.Row, row)


def _load_job_source_root(conn: sqlite3.Connection, job_id: JobId) -> str:
    """
    Return the authoritative Job-owned source root for a job.

    Parameters
    ----------
    conn:
        Open SQLite connection.
    job_id:
        Identifier of the job whose binding root should be loaded.

    Returns
    -------
    str
        Authoritative Job-owned source root.

    Raises
    ------
    UnknownJobError
        If the job does not exist or has been soft deleted.
    """

    row = _load_existing_job_row(conn, job_id)
    source_root_raw = row["source_root"]
    return str(source_root_raw) if source_root_raw is not None else ""


def _ensure_template_backup_policy_row(
    conn: sqlite3.Connection,
    *,
    template_id: str,
) -> None:
    """
    Ensure a Template-owned compression policy row exists.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open profile store connection.
    template_id : str
        Template identifier that must have a policy row.

    Notes
    -----
    Older or partially initialized local profiles can contain valid Job ->
    Template bindings before the companion ``template_backup_policy`` row was
    written. The authoritative Template policy default remains ``"none"``, so
    inserting that row preserves the current model while allowing later lookups
    to succeed deterministically.
    """
    conn.execute(
        "INSERT INTO template_backup_policy(template_id, compression) "
        "VALUES(?, 'none') "
        "ON CONFLICT(template_id) DO NOTHING",
        (template_id,),
    )


@dataclass(frozen=True, slots=True)
class SqliteProfileStore(ProfileStore):
    """
    SQLite-backed ProfileStore.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.

    Notes
    -----
    The database file is created if absent. The parent directory is created as
    needed.
    """

    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA_V1)
            _ensure_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def list_jobs(self) -> Sequence[JobSummary]:
        """See ProfileStore.list_jobs."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT job_id, name FROM jobs WHERE is_deleted = 0 ORDER BY name ASC"
            ).fetchall()
        return [JobSummary(job_id=str(r["job_id"]), name=str(r["name"])) for r in rows]

    def load_job_binding(self, job_id: JobId) -> JobBinding:
        """See ProfileStore.load_job_binding."""
        with self._connect() as conn:
            _ensure_schema(conn)
            row = _load_existing_job_row(conn, job_id)
            template_id = _load_template_id(conn, job_id)
            source_root = _load_job_source_root(conn, job_id)

        return JobBinding(
            job_id=str(row["job_id"]),
            job_name=str(row["name"]),
            template_id=template_id,
            source_root=source_root,
        )

    def save_job_binding(self, binding: JobBinding) -> None:
        """See ProfileStore.save_job_binding."""
        job_name = binding.job_name.strip()
        template_id = binding.template_id.strip()
        if not job_name:
            raise ValueError("job_name must not be empty.")
        if not template_id:
            raise ValueError("template_id must not be empty.")

        with self._connect() as conn:
            _ensure_schema(conn)
            cur = conn.execute(
                "UPDATE jobs SET name = ?, template_id = ?, source_root = ? "
                "WHERE job_id = ? AND is_deleted = 0",
                (job_name, template_id, binding.source_root, binding.job_id),
            )
            if cur.rowcount == 0:
                raise UnknownJobError(f"Unknown job_id: {binding.job_id}")
            _ensure_template_backup_policy_row(conn, template_id=template_id)

    def load_restore_defaults(self, job_id: JobId) -> tuple[str | None, str | None]:
        """
        Load persisted Restore tab defaults for a job.

        Returns
        -------
        tuple[str | None, str | None]
            (archive_root, restore_dest_root)
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT archive_root, restore_dest_root FROM jobs "
                "WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if row is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")
            return (
                str(row["archive_root"]) if row["archive_root"] is not None else None,
                str(row["restore_dest_root"]) if row["restore_dest_root"] is not None else None,
            )

    def save_restore_defaults(
        self,
        job_id: JobId,
        *,
        archive_root: str | None,
        restore_dest_root: str | None,
    ) -> None:
        """
        Persist Restore tab defaults for a job.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            cur = conn.execute(
                "UPDATE jobs SET archive_root = ?, restore_dest_root = ? "
                "WHERE job_id = ? AND is_deleted = 0",
                (archive_root, restore_dest_root, job_id),
            )
            if cur.rowcount == 0:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

    def create_job(self, name: str) -> JobId:
        """
        Create a job and return its stable identifier.

        Job identifiers are derived deterministically using UUID5 over
        (profile_id, initial_job_name).
        """
        canonical = _canonicalize_job_name(name)
        with self._connect() as conn:
            profile_id = _get_or_create_profile_id(conn)
            job_id: JobId = uuid.uuid5(JOB_ID_NAMESPACE, f"{profile_id}:{canonical}").hex
            template_id = _new_template_id()

            # Insert new job, or revive if previously soft-deleted.
            conn.execute(
                "INSERT INTO jobs(job_id, name, template_id, source_root, is_deleted) "
                "VALUES(?, ?, ?, '', 0) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "name = excluded.name, "
                "is_deleted = 0",
                (job_id, name, template_id),
            )
            conn.execute(
                "UPDATE jobs SET source_root = COALESCE(source_root, '') "
                "WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            )
            current_template_id = _load_template_id(conn, job_id)
            _ensure_template_backup_policy_row(conn, template_id=current_template_id)
        return job_id

    def rename_job(self, job_id: JobId, new_name: str) -> None:
        """Rename an existing job without changing its identifier."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE jobs SET name = ? WHERE job_id = ? AND is_deleted = 0",
                (new_name, job_id),
            )
            if cur.rowcount == 0:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

    def delete_job(self, job_id: JobId) -> None:
        """Soft delete an existing job."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE jobs SET is_deleted = 1 WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            )
            if cur.rowcount == 0:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

    def load_template_selection_rules(self, job_id: JobId) -> TemplateSelectionRules:
        """See ProfileStore.load_template_selection_rules."""
        with self._connect() as conn:
            _ensure_schema(conn)
            job = conn.execute(
                "SELECT job_id FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if job is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

            template_id = _load_template_id(conn, job_id)
            inc = conn.execute(
                "SELECT pattern FROM template_selection_rules "
                "WHERE template_id = ? AND kind = 'include' ORDER BY position ASC",
                (template_id,),
            ).fetchall()
            exc = conn.execute(
                "SELECT pattern FROM template_selection_rules "
                "WHERE template_id = ? AND kind = 'exclude' ORDER BY position ASC",
                (template_id,),
            ).fetchall()

        return TemplateSelectionRules(
            include=tuple(str(row["pattern"]) for row in inc),
            exclude=tuple(str(row["pattern"]) for row in exc),
        )

    def save_template_selection_rules(
        self,
        job_id: JobId,
        name: str,
        selection_rules: TemplateSelectionRules,
    ) -> None:
        """See ProfileStore.save_template_selection_rules."""
        normalized = normalize_template_selection_rules(selection_rules)

        with self._connect() as conn:
            _ensure_schema(conn)
            _ = name
            _load_existing_job_row(conn, job_id)

            template_id = _load_template_id(conn, job_id)
            conn.execute(
                "DELETE FROM template_selection_rules "
                "WHERE template_id = ? AND kind IN ('include','exclude')",
                (template_id,),
            )
            for idx, pattern in enumerate(normalized.include):
                conn.execute(
                    "INSERT INTO template_selection_rules(template_id, kind, pattern, position) "
                    "VALUES(?, 'include', ?, ?)",
                    (template_id, pattern, idx),
                )
            for idx, pattern in enumerate(normalized.exclude):
                conn.execute(
                    "INSERT INTO template_selection_rules(template_id, kind, pattern, position) "
                    "VALUES(?, 'exclude', ?, ?)",
                    (template_id, pattern, idx),
                )

    def load_template_compression(self, job_id: JobId) -> str:
        """See ProfileStore.load_template_compression."""
        with self._connect() as conn:
            _ensure_schema(conn)
            job = conn.execute(
                "SELECT job_id FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if job is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

            template_id = _load_template_id(conn, job_id)
            _ensure_template_backup_policy_row(conn, template_id=template_id)
            row = conn.execute(
                "SELECT compression FROM template_backup_policy WHERE template_id = ?",
                (template_id,),
            ).fetchone()
            if row is None:
                raise UnknownJobError(f"No template compression found for job_id: {job_id}")

        return _normalize_template_compression(str(row["compression"]))

    def save_template_compression(self, job_id: JobId, name: str, compression: str) -> None:
        """See ProfileStore.save_template_compression."""
        normalized = _normalize_template_compression(compression)

        with self._connect() as conn:
            _ensure_schema(conn)
            _ = name
            _load_existing_job_row(conn, job_id)

            template_id = _load_template_id(conn, job_id)
            conn.execute(
                "INSERT INTO template_backup_policy(template_id, compression) VALUES(?, ?) "
                "ON CONFLICT(template_id) DO UPDATE SET compression = excluded.compression",
                (template_id, normalized),
            )

    def load_backup_schedule(self, job_id: JobId) -> BackupScheduleSpec:
        """
        Load the persisted scheduled-task record for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job to load.

        Returns
        -------
            BackupScheduleSpec
                Persisted scheduled-task record.

        Raises
        ------
        UnknownJobError
            If the job or schedule is not present.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT js.job_id, js.cadence, js.start_time_local, js.weekdays "
                "FROM job_schedules js "
                "JOIN jobs j ON j.job_id = js.job_id "
                "WHERE js.job_id = ? AND j.is_deleted = 0",
                (job_id,),
            ).fetchone()
            if row is None:
                raise UnknownJobError(f"No backup schedule found for job_id: {job_id}")

        weekdays_raw = str(row["weekdays"]) if row["weekdays"] is not None else ""
        weekdays = tuple(part for part in weekdays_raw.split(",") if part)
        source_root_value = self.load_job_binding(job_id).source_root
        compression_value = self.load_template_compression(job_id)

        return normalize_schedule_spec(
            BackupScheduleSpec(
                job_id=str(row["job_id"]),
                source_root=str(source_root_value or ""),
                cadence=str(row["cadence"]),
                start_time_local=str(row["start_time_local"]),
                weekdays=weekdays,
                compression=str(compression_value or ""),
            )
        )

    def save_backup_schedule(self, schedule: BackupScheduleSpec) -> None:
        """
        Persist the scheduled-task record for an existing job.

        Parameters
        ----------
        schedule:
            Scheduled-task record to persist.

        Raises
        ------
        UnknownJobError
            If the target job does not exist.
        """
        normalized = normalize_schedule_spec(schedule)
        with self._connect() as conn:
            _ensure_schema(conn)
            job = conn.execute(
                "SELECT job_id, name FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (normalized.job_id,),
            ).fetchone()
            if job is None:
                raise UnknownJobError(f"Unknown job_id: {normalized.job_id}")

            conn.execute(
                "INSERT INTO job_schedules(job_id, cadence, start_time_local, weekdays) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "cadence = excluded.cadence, "
                "start_time_local = excluded.start_time_local, "
                "weekdays = excluded.weekdays",
                (
                    normalized.job_id,
                    normalized.cadence,
                    normalized.start_time_local,
                    ",".join(normalized.weekdays) if normalized.weekdays else None,
                ),
            )
            conn.execute(
                "UPDATE jobs SET source_root = ? WHERE job_id = ? AND is_deleted = 0",
                (normalized.source_root, normalized.job_id),
            )

        self.save_template_compression(
            job_id=normalized.job_id,
            name=str(job["name"]),
            compression=normalized.compression,
        )

    def delete_backup_schedule(self, job_id: JobId) -> None:
        """
        Delete the persisted backup schedule for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose schedule should be removed.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute("DELETE FROM job_schedules WHERE job_id = ?", (job_id,))


def open_profile_store(profile_name: str, data_root: Path | None = None) -> SqliteProfileStore:
    """
    Convenience constructor that ensures profile directories exist.

    Parameters
    ----------
    profile_name:
        Name of the WCBT profile.
    data_root:
        Optional override for the WCBT data root.

    Returns
    -------
    SqliteProfileStore
        Ready-to-use SQLite-backed store.
    """
    paths = resolve_profile_paths(profile_name=profile_name, data_root=data_root)
    ensure_profile_directories(paths)
    return SqliteProfileStore(db_path=paths.index_root / "profiles.sqlite")


def validate_profile_store_contract(profile_name: str, data_root: Path | None = None) -> None:
    """
    Validate that the persisted profile database satisfies the current contract.

    Parameters
    ----------
    profile_name:
        Name of the WCBT profile.
    data_root:
        Optional override for the WCBT data root.

    Raises
    ------
    IncompatibleProfileStoreError
        If the persisted database predates the current contract.
    """

    database_path = profile_store_db_path(profile_name=profile_name, data_root=data_root)
    if not database_path.exists():
        return

    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT job_id, template_id FROM jobs WHERE is_deleted = 0 ORDER BY name ASC"
        ).fetchall()
        for row in rows:
            template_id_raw = row["template_id"]
            if template_id_raw is None or not str(template_id_raw).strip():
                raise IncompatibleProfileStoreError(
                    _build_incompatible_contract_message(
                        job_id=str(row["job_id"]),
                        missing_field="template_id",
                    )
                )
    finally:
        conn.close()


def _canonicalize_job_name(name: str) -> str:
    parts = name.split()
    return " ".join(parts).casefold()
