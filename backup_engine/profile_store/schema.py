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
    template_id TEXT,
    source_root TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0
);

-- Authoritative Template-owned selection rules.
CREATE TABLE IF NOT EXISTS template_selection_rules (
    template_id TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK(kind IN ('include','exclude')),
    pattern     TEXT NOT NULL,
    position    INTEGER NOT NULL,
    PRIMARY KEY (template_id, kind, position)
);

CREATE INDEX IF NOT EXISTS idx_template_selection_rules_kind
ON template_selection_rules(template_id, kind);

-- Authoritative Template-owned compression policy.
CREATE TABLE IF NOT EXISTS template_backup_policy (
    template_id  TEXT PRIMARY KEY,
    compression  TEXT NOT NULL DEFAULT 'none'
);

CREATE TABLE IF NOT EXISTS job_schedules (
    job_id            TEXT PRIMARY KEY,
    cadence           TEXT NOT NULL CHECK(cadence IN ('daily','weekly')),
    start_time_local  TEXT NOT NULL,
    weekdays          TEXT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
"""
