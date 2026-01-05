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
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..paths_and_safety import ProfilePaths, ensure_profile_directories, resolve_profile_paths
from .api import JobId, JobSummary, ProfileStore, RuleSet
from .errors import InvalidRuleError, UnknownJobError
from .schema import SCHEMA_V1


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

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def list_jobs(self) -> Sequence[JobSummary]:
        """See ProfileStore.list_jobs."""
        with self._connect() as conn:
            rows = conn.execute("SELECT job_id, name FROM jobs ORDER BY name ASC").fetchall()
        return [JobSummary(job_id=str(r["job_id"]), name=str(r["name"])) for r in rows]

    def load_rules(self, job_id: JobId) -> RuleSet:
        """See ProfileStore.load_rules."""
        with self._connect() as conn:
            job = conn.execute("SELECT job_id FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
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
            conn.execute(
                "INSERT INTO jobs(job_id, name) VALUES(?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET name = excluded.name",
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
