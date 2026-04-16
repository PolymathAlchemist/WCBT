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

-- Transitional Job-shaped carrier for Template-owned selection rules.
CREATE TABLE IF NOT EXISTS rules (
    job_id   TEXT NOT NULL,
    kind     TEXT NOT NULL CHECK(kind IN ('include','exclude')),
    pattern  TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (job_id, kind, position),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rules_job_kind ON rules(job_id, kind);

CREATE TABLE IF NOT EXISTS job_schedules (
    job_id            TEXT PRIMARY KEY,
    cadence           TEXT NOT NULL CHECK(cadence IN ('daily','weekly')),
    start_time_local  TEXT NOT NULL,
    weekdays          TEXT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

-- Transitional schedule-side carrier. Scheduling does not own these fields.
CREATE TABLE IF NOT EXISTS scheduled_backup_legacy_inputs (
    job_id       TEXT PRIMARY KEY,
    source_root  TEXT NOT NULL,
    compression  TEXT NOT NULL DEFAULT 'none',
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

-- Transitional Job-shaped carrier for mixed ownership:
-- source_root remains Job-owned target binding;
-- compression is Template-owned policy mirrored for compatibility only.
CREATE TABLE IF NOT EXISTS job_backup_defaults (
    job_id       TEXT PRIMARY KEY,
    source_root  TEXT NOT NULL,
    compression  TEXT NOT NULL DEFAULT 'none',
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
"""
