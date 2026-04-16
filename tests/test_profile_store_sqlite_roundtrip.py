from __future__ import annotations

import sqlite3
from pathlib import Path

from backup_engine.profile_store.api import JobBackupDefaults, RuleSet
from backup_engine.profile_store.sqlite_store import SqliteProfileStore
from backup_engine.template_policy import TemplateSelectionRules


def test_sqlite_profile_store_roundtrip(tmp_path: Path) -> None:
    """Rules saved for a job_id should round-trip exactly."""
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    store.save_rules(
        job_id="job-1", name="Example Job", rules=RuleSet(include=("a/**",), exclude=("b/**",))
    )
    loaded = store.load_rules("job-1")

    assert loaded == RuleSet(include=("a/**",), exclude=("b/**",))


def test_template_selection_rules_are_authoritative_and_rules_table_is_mirrored(
    tmp_path: Path,
) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    store.save_template_selection_rules(
        job_id="job-1",
        name="Example Job",
        selection_rules=TemplateSelectionRules(include=("a/**",), exclude=("b/**",)),
    )

    loaded = store.load_template_selection_rules("job-1")
    mirrored = store.load_rules("job-1")

    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        template_rows = connection.execute(
            "SELECT kind, pattern, position FROM template_selection_rules "
            "WHERE template_id = ? ORDER BY kind ASC, position ASC",
            ("job-1",),
        ).fetchall()
        mirror_rows = connection.execute(
            "SELECT kind, pattern, position FROM rules WHERE job_id = ? ORDER BY kind ASC, position ASC",
            ("job-1",),
        ).fetchall()
    finally:
        connection.close()

    assert loaded == TemplateSelectionRules(include=("a/**",), exclude=("b/**",))
    assert mirrored == RuleSet(include=("a/**",), exclude=("b/**",))
    assert [(str(row["kind"]), str(row["pattern"])) for row in template_rows] == [
        ("exclude", "b/**"),
        ("include", "a/**"),
    ]
    assert [(str(row["kind"]), str(row["pattern"])) for row in mirror_rows] == [
        ("exclude", "b/**"),
        ("include", "a/**"),
    ]


def test_template_compression_is_authoritative_and_job_defaults_are_mirrored(
    tmp_path: Path,
) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")

    store.save_job_backup_defaults(
        job_id,
        defaults=JobBackupDefaults(source_root="C:/games/world", compression="zip"),
    )

    loaded_defaults = store.load_job_backup_defaults(job_id)
    loaded_compression = store.load_template_compression(job_id)

    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        template_row = connection.execute(
            "SELECT compression FROM template_backup_policy WHERE template_id = ?",
            (job_id,),
        ).fetchone()
        mirror_row = connection.execute(
            "SELECT source_root, compression FROM job_backup_defaults WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    finally:
        connection.close()

    assert loaded_compression == "zip"
    assert loaded_defaults == JobBackupDefaults(source_root="C:/games/world", compression="zip")
    assert template_row is not None
    assert str(template_row["compression"]) == "zip"
    assert mirror_row is not None
    assert str(mirror_row["source_root"]) == "C:/games/world"
    assert str(mirror_row["compression"]) == "zip"
