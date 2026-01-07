"""
SQLite implementation of ProfileStore.

This module owns the on-disk persistence format for per-job authoring rules.

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

from ..paths_and_safety import ProfilePaths, ensure_profile_directories, resolve_profile_paths
from .api import JobId, JobSummary, ProfileStore, RuleSet
from .errors import InvalidRuleError, UnknownJobError
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
        with self._connect() as conn:
            job = conn.execute(
                "SELECT job_id FROM jobs WHERE job_id = ? AND is_deleted = 0",
                (job_id,),
            ).fetchone()
            if job is None:
                raise UnknownJobError(f"Unknown job_id: {job_id}")

            inc = conn.execute(
                "SELECT pattern FROM rules WHERE job_id = ? AND kind = 'include' ORDER BY position ASC",
                (job_id,),
            ).fetchall()
            exc = conn.execute(
                "SELECT pattern FROM rules WHERE job_id = ? AND kind = 'exclude' ORDER BY position ASC",
                (job_id,),
            ).fetchall()

        return RuleSet(
            include=tuple(str(r["pattern"]) for r in inc),
            exclude=tuple(str(r["pattern"]) for r in exc),
        )

    def save_rules(self, job_id: JobId, name: str, rules: RuleSet) -> None:
        """See ProfileStore.save_rules."""
        normalized = RuleSet(
            include=_normalize_patterns(rules.include),
            exclude=_normalize_patterns(rules.exclude),
        )

        with self._connect() as conn:
            _ensure_schema(conn)
            conn.execute(
                "INSERT INTO jobs(job_id, name, is_deleted) VALUES(?, ?, 0) "
                "ON CONFLICT(job_id) DO UPDATE SET name = excluded.name, is_deleted = 0",
                (job_id, name),
            )

            conn.execute(
                "DELETE FROM rules WHERE job_id = ? AND kind IN ('include','exclude')", (job_id,)
            )

            for idx, pat in enumerate(normalized.include):
                conn.execute(
                    "INSERT INTO rules(job_id, kind, pattern, position) VALUES(?, 'include', ?, ?)",
                    (job_id, pat, idx),
                )
            for idx, pat in enumerate(normalized.exclude):
                conn.execute(
                    "INSERT INTO rules(job_id, kind, pattern, position) VALUES(?, 'exclude', ?, ?)",
                    (job_id, pat, idx),
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
