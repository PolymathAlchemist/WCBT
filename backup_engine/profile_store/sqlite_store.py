"""
SQLite implementation of ProfileStore.

This module owns the on-disk persistence format for per-job authoring rules.

Notes
-----
Template is the authoritative owner of selection rules and compression policy.
This store still persists Job-shaped compatibility carriers for those fields so
current runtime and authoring paths remain stable during the boundary
introduction sequence.

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
from typing import Final, Sequence

from backup_engine.scheduling.models import BackupScheduleSpec, normalize_schedule_spec
from backup_engine.template_policy import TemplateSelectionRules

from ..paths_and_safety import ProfilePaths, ensure_profile_directories, resolve_profile_paths
from .api import JobBackupDefaults, JobId, JobSummary, ProfileStore, RuleSet
from .errors import InvalidRuleError, UnknownJobError
from .rules import normalize_rules, normalize_template_selection_rules
from .schema import SCHEMA_V1

# Stable UUID namespace for JobId derivation.
# Changing this will invalidate all persisted job identities.
JOB_ID_NAMESPACE: Final[uuid.UUID] = uuid.UUID("6b0e2c7a-8e8f-4c5a-bb5d-9e6c6a3e7a01")


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


def _normalize_patterns(values: Sequence[str]) -> tuple[str, ...]:
    """
    Normalize user-authored glob patterns.

    Invariants
    ----------
    - Root-relative: must not start with '/' and must not contain a drive hint ':'
    - Uses '/' separators (backslashes are normalized to '/')
    - Empty patterns are dropped after stripping

    Parameters
    ----------
    values:
        Raw patterns from UI input.

    Returns
    -------
    tuple[str, ...]
        Normalized patterns, preserving order.

    Raises
    ------
    InvalidRuleError
        If any pattern violates invariants.
    """
    out: list[str] = []
    for raw in values:
        cleaned = str(raw).strip().replace("\\", "/")
        if not cleaned:
            continue
        if cleaned.startswith("/"):
            raise InvalidRuleError("Patterns must be root-relative and must not start with '/'.")
        if ":" in cleaned:
            raise InvalidRuleError("Patterns must be root-relative and must not contain ':'.")
        out.append(cleaned)
    return tuple(out)


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

    # Ensure the trigger-only schedule table and transitional compatibility
    # table exist, even for databases created before the scheduling boundary
    # was split.
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
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scheduled_backup_legacy_inputs ("
        "job_id TEXT PRIMARY KEY, "
        "source_root TEXT NOT NULL, "
        "compression TEXT NOT NULL DEFAULT 'none', "
        "FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS job_backup_defaults ("
        "job_id TEXT PRIMARY KEY, "
        "source_root TEXT NOT NULL, "
        "compression TEXT NOT NULL DEFAULT 'none', "
        "FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE)"
    )

    # Ensure jobs has is_deleted column for soft deletion.
    cols = conn.execute("PRAGMA table_info(jobs)").fetchall()
    col_names = {str(r["name"]) for r in cols}
    if "is_deleted" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")

    # Optional per-job UI state (restore tab).
    if "archive_root" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN archive_root TEXT NULL")
    if "restore_dest_root" not in col_names:
        conn.execute("ALTER TABLE jobs ADD COLUMN restore_dest_root TEXT NULL")


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


def _current_template_id_for_job(job_id: JobId) -> str:
    """
    Return the current Template routing key for a job.

    Notes
    -----
    The current relocation step establishes Template-owned authority for
    selection rules before full Job narrowing exists. Until a later boundary
    move introduces explicit Template identity, the current job identifier is
    reused only as a routing key into Template-owned rule storage.
    """

    return job_id


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

            # Insert new job, or revive if previously soft-deleted.
            conn.execute(
                "INSERT INTO jobs(job_id, name, is_deleted) VALUES(?, ?, 0) "
                "ON CONFLICT(job_id) DO UPDATE SET name = excluded.name, is_deleted = 0",
                (job_id, name),
            )
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

    def load_rules(self, job_id: JobId) -> RuleSet:
        """See ProfileStore.load_rules."""
        selection_rules = self.load_template_selection_rules(job_id)
        return RuleSet(
            include=selection_rules.include,
            exclude=selection_rules.exclude,
        )

    def save_rules(self, job_id: JobId, name: str, rules: RuleSet) -> None:
        """See ProfileStore.save_rules."""
        self.save_template_selection_rules(
            job_id=job_id,
            name=name,
            selection_rules=rules.to_template_selection_rules(),
        )

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

            template_id = _current_template_id_for_job(job_id)
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

            if not inc and not exc:
                inc = conn.execute(
                    "SELECT pattern FROM rules WHERE job_id = ? AND kind = 'include' ORDER BY position ASC",
                    (job_id,),
                ).fetchall()
                exc = conn.execute(
                    "SELECT pattern FROM rules WHERE job_id = ? AND kind = 'exclude' ORDER BY position ASC",
                    (job_id,),
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
        mirror_rules = normalize_rules(
            RuleSet(include=normalized.include, exclude=normalized.exclude)
        )

        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute(
                "INSERT INTO jobs(job_id, name, is_deleted) VALUES(?, ?, 0) "
                "ON CONFLICT(job_id) DO UPDATE SET name = excluded.name, is_deleted = 0",
                (job_id, name),
            )

            template_id = _current_template_id_for_job(job_id)
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

            conn.execute(
                "DELETE FROM rules WHERE job_id = ? AND kind IN ('include','exclude')", (job_id,)
            )

            for idx, pat in enumerate(mirror_rules.include):
                conn.execute(
                    "INSERT INTO rules(job_id, kind, pattern, position) VALUES(?, 'include', ?, ?)",
                    (job_id, pat, idx),
                )
            for idx, pat in enumerate(mirror_rules.exclude):
                conn.execute(
                    "INSERT INTO rules(job_id, kind, pattern, position) VALUES(?, 'exclude', ?, ?)",
                    (job_id, pat, idx),
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

            template_id = _current_template_id_for_job(job_id)
            row = conn.execute(
                "SELECT compression FROM template_backup_policy WHERE template_id = ?",
                (template_id,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT compression FROM job_backup_defaults WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT compression FROM scheduled_backup_legacy_inputs WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
            if row is None:
                raise UnknownJobError(f"No template compression found for job_id: {job_id}")

        return _normalize_template_compression(str(row["compression"]))

    def save_template_compression(self, job_id: JobId, name: str, compression: str) -> None:
        """See ProfileStore.save_template_compression."""
        normalized = _normalize_template_compression(compression)

        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute(
                "INSERT INTO jobs(job_id, name, is_deleted) VALUES(?, ?, 0) "
                "ON CONFLICT(job_id) DO UPDATE SET name = excluded.name, is_deleted = 0",
                (job_id, name),
            )

            template_id = _current_template_id_for_job(job_id)
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
            Persisted scheduled-task record with trigger metadata plus any
            transitional compatibility payload still required by the current
            scheduled execution path. Any attached ``compression`` value remains
            Template-owned policy mirrored here for compatibility only.

        Raises
        ------
        UnknownJobError
            If the job or schedule is not present.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT js.job_id, js.cadence, js.start_time_local, js.weekdays, "
                "job_defaults.source_root AS job_source_root, "
                "job_defaults.compression AS job_compression "
                "FROM job_schedules js "
                "JOIN jobs j ON j.job_id = js.job_id "
                "LEFT JOIN job_backup_defaults job_defaults ON job_defaults.job_id = js.job_id "
                "WHERE js.job_id = ? AND j.is_deleted = 0",
                (job_id,),
            ).fetchone()
            if row is None:
                raise UnknownJobError(f"No backup schedule found for job_id: {job_id}")

            legacy_row = conn.execute("PRAGMA table_info(job_schedules)").fetchall()
            legacy_columns = {str(column["name"]) for column in legacy_row}
            fallback_row = None
            if "source_root" in legacy_columns or "compression" in legacy_columns:
                fallback_row = conn.execute(
                    "SELECT source_root, compression FROM job_schedules WHERE job_id = ?",
                    (job_id,),
                ).fetchone()

        weekdays_raw = str(row["weekdays"]) if row["weekdays"] is not None else ""
        weekdays = tuple(part for part in weekdays_raw.split(",") if part)
        source_root_value = row["job_source_root"]
        compression_value = self.load_template_compression(job_id)

        if source_root_value is None and fallback_row is not None:
            source_root_value = fallback_row["source_root"]

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
            Scheduled-task record to persist. The scheduling boundary owns the
            trigger metadata only; any backup-definition payload remains
            compatibility state until later refactor steps remove it. In that
            payload, ``source_root`` remains Job-owned target binding and
            ``compression`` remains Template-owned policy.

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

        self.save_template_compression(
            job_id=normalized.job_id,
            name=str(job["name"]),
            compression=normalized.compression,
        )

        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute(
                "INSERT INTO job_backup_defaults(job_id, source_root, compression) "
                "VALUES(?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "source_root = excluded.source_root, "
                "compression = excluded.compression",
                (
                    normalized.job_id,
                    normalized.source_root,
                    normalized.compression,
                ),
            )

            conn.execute(
                "INSERT INTO scheduled_backup_legacy_inputs(job_id, source_root, compression) "
                "VALUES(?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "source_root = excluded.source_root, "
                "compression = excluded.compression",
                (
                    normalized.job_id,
                    normalized.source_root,
                    normalized.compression,
                ),
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
            conn.execute("DELETE FROM scheduled_backup_legacy_inputs WHERE job_id = ?", (job_id,))
            conn.execute("DELETE FROM job_schedules WHERE job_id = ?", (job_id,))

    def load_job_backup_defaults(self, job_id: JobId) -> JobBackupDefaults:
        """
        Load the current Job-shaped compatibility carrier for backup inputs.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            row = conn.execute(
                "SELECT defaults.source_root, defaults.compression "
                "FROM job_backup_defaults defaults "
                "JOIN jobs j ON j.job_id = defaults.job_id "
                "WHERE defaults.job_id = ? AND j.is_deleted = 0",
                (job_id,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT source_root, compression FROM scheduled_backup_legacy_inputs "
                    "WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
            if row is None:
                raise UnknownJobError(f"No backup defaults found for job_id: {job_id}")

        compression = self.load_template_compression(job_id)
        return JobBackupDefaults(
            source_root=str(row["source_root"]),
            compression=compression,
        )

    def save_job_backup_defaults(self, job_id: JobId, defaults: JobBackupDefaults) -> None:
        """
        Persist the current Job-shaped compatibility carrier for backup inputs.

        Notes
        -----
        This storage path is transitional. ``source_root`` remains Job-owned
        target binding, and ``compression`` remains Template-owned policy.
        """
        with self._connect() as conn:
            _ensure_schema(conn)
            job = conn.execute(
                "SELECT job_id FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if job is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

            job_name_row = conn.execute(
                "SELECT name FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if job_name_row is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

        self.save_template_compression(
            job_id=job_id,
            name=str(job_name_row["name"]),
            compression=defaults.compression,
        )

        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute(
                "INSERT INTO job_backup_defaults(job_id, source_root, compression) "
                "VALUES(?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "source_root = excluded.source_root, "
                "compression = excluded.compression",
                (
                    job_id,
                    defaults.source_root,
                    _normalize_template_compression(defaults.compression),
                ),
            )


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


def _canonicalize_job_name(name: str) -> str:
    parts = name.split()
    return " ".join(parts).casefold()
