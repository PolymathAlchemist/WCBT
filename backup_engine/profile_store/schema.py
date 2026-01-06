"""SQLite schema for ProfileStore.

Notes
-----
The profile store database is placed under ProfilePaths.index_root because it is
considered rebuildable index state (not canonical source-of-truth data).
"""

from __future__ import annotations

SCHEMA_V1 = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS profile_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id     TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rules (
    job_id   TEXT NOT NULL,
    kind     TEXT NOT NULL CHECK(kind IN ('include','exclude')),
    pattern  TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (job_id, kind, position),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rules_job_kind ON rules(job_id, kind);
"""
