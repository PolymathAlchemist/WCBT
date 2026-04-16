from __future__ import annotations

import sqlite3
from pathlib import Path

from backup_engine.profile_store.api import RuleSet
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
