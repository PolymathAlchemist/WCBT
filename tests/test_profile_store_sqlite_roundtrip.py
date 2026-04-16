from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backup_engine.profile_store.api import JobBackupDefaults, RuleSet
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import SqliteProfileStore
from backup_engine.template_policy import TemplateSelectionRules


def test_sqlite_profile_store_roundtrip(tmp_path: Path) -> None:
    """Rules saved for a job_id should round-trip exactly."""
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")

    store.save_rules(
        job_id=job_id,
        name="Example Job",
        rules=RuleSet(include=("a/**",), exclude=("b/**",)),
    )
    loaded = store.load_rules(job_id)

    assert loaded == RuleSet(include=("a/**",), exclude=("b/**",))


def test_template_selection_rules_are_authoritative_and_rules_view_is_derived(
    tmp_path: Path,
) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

    store.save_template_selection_rules(
        job_id=job_id,
        name="Example Job",
        selection_rules=TemplateSelectionRules(include=("a/**",), exclude=("b/**",)),
    )

    loaded = store.load_template_selection_rules(job_id)
    mirrored = store.load_rules(job_id)

    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        template_rows = connection.execute(
            "SELECT kind, pattern, position FROM template_selection_rules "
            "WHERE template_id = ? ORDER BY kind ASC, position ASC",
            (binding.template_id,),
        ).fetchall()
        retired_mirror_rows = connection.execute(
            "SELECT kind, pattern, position FROM rules WHERE job_id = ? ORDER BY kind ASC, position ASC",
            (job_id,),
        ).fetchall()
    finally:
        connection.close()

    assert loaded == TemplateSelectionRules(include=("a/**",), exclude=("b/**",))
    assert mirrored == RuleSet(include=("a/**",), exclude=("b/**",))
    assert [(str(row["kind"]), str(row["pattern"])) for row in template_rows] == [
        ("exclude", "b/**"),
        ("include", "a/**"),
    ]
    assert retired_mirror_rows == []


def test_template_compression_is_authoritative_and_backup_defaults_view_is_derived(
    tmp_path: Path,
) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

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
            (binding.template_id,),
        ).fetchone()
        retired_mirror_row = connection.execute(
            "SELECT source_root, compression FROM job_backup_defaults WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    finally:
        connection.close()

    assert loaded_compression == "zip"
    assert loaded_defaults == JobBackupDefaults(source_root="C:/games/world", compression="zip")
    assert template_row is not None
    assert str(template_row["compression"]) == "zip"
    assert retired_mirror_row is None
    assert store.load_job_binding(job_id).source_root == "C:/games/world"


def test_create_job_initializes_binding_without_template_policy(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

    assert binding.job_id == job_id
    assert binding.job_name == "Example Job"
    assert binding.template_id
    assert binding.template_id != job_id
    assert binding.source_root == ""


def test_existing_job_gets_independent_template_id_without_migrating_legacy_policy(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "profiles.sqlite"
    store = SqliteProfileStore(db_path=database_path)
    job_id = store.create_job("Example Job")

    connection = sqlite3.connect(database_path)
    try:
        connection.execute("UPDATE jobs SET template_id = NULL WHERE job_id = ?", (job_id,))
        connection.execute(
            "INSERT INTO template_selection_rules(template_id, kind, pattern, position) "
            "VALUES(?, 'include', ?, 0)",
            (job_id, "legacy/**"),
        )
        connection.execute(
            "INSERT INTO template_backup_policy(template_id, compression) VALUES(?, ?)",
            (job_id, "zip"),
        )
        connection.commit()
    finally:
        connection.close()

    binding = store.load_job_binding(job_id)

    assert binding.template_id
    assert binding.template_id != job_id
    assert store.load_template_selection_rules(job_id) == TemplateSelectionRules(
        include=("legacy/**",),
        exclude=(),
    )
    assert store.load_template_compression(job_id) == "zip"


def test_template_policy_save_requires_existing_job_binding(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    with pytest.raises(UnknownJobError):
        store.save_template_selection_rules(
            job_id="missing-job",
            name="Missing Job",
            selection_rules=TemplateSelectionRules(include=("a/**",), exclude=("b/**",)),
        )

    with pytest.raises(UnknownJobError):
        store.save_template_compression(
            job_id="missing-job",
            name="Missing Job",
            compression="zip",
        )


def test_legacy_rules_and_defaults_are_promoted_to_authoritative_storage(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "profiles.sqlite"
    store = SqliteProfileStore(db_path=database_path)
    job_id = store.create_job("Example Job")

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("UPDATE jobs SET source_root = NULL WHERE job_id = ?", (job_id,))
        connection.execute(
            "INSERT INTO rules(job_id, kind, pattern, position) VALUES(?, 'include', ?, 0)",
            (job_id, "legacy/**"),
        )
        connection.execute(
            "INSERT INTO job_backup_defaults(job_id, source_root, compression) VALUES(?, ?, ?)",
            (job_id, "C:/legacy/world", "zip"),
        )
        connection.commit()
    finally:
        connection.close()

    binding = store.load_job_binding(job_id)
    selection_rules = store.load_template_selection_rules(job_id)
    defaults = store.load_job_backup_defaults(job_id)

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        promoted_job_row = connection.execute(
            "SELECT source_root FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        promoted_template_rows = connection.execute(
            "SELECT kind, pattern FROM template_selection_rules "
            "WHERE template_id = ? ORDER BY kind ASC, position ASC",
            (binding.template_id,),
        ).fetchall()
        promoted_template_policy = connection.execute(
            "SELECT compression FROM template_backup_policy WHERE template_id = ?",
            (binding.template_id,),
        ).fetchone()
    finally:
        connection.close()

    assert binding.source_root == "C:/legacy/world"
    assert selection_rules == TemplateSelectionRules(include=("legacy/**",), exclude=())
    assert defaults == JobBackupDefaults(source_root="C:/legacy/world", compression="zip")
    assert promoted_job_row is not None
    assert str(promoted_job_row["source_root"]) == "C:/legacy/world"
    assert [(str(row["kind"]), str(row["pattern"])) for row in promoted_template_rows] == [
        ("include", "legacy/**"),
    ]
    assert promoted_template_policy is not None
    assert str(promoted_template_policy["compression"]) == "zip"
