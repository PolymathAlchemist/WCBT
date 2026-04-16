from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.errors import IncompatibleProfileStoreError, UnknownJobError
from backup_engine.profile_store.sqlite_store import (
    SqliteProfileStore,
    open_profile_store,
    profile_store_db_path,
    validate_profile_store_contract,
)
from backup_engine.template_policy import TemplateSelectionRules


def test_template_selection_rules_roundtrip(tmp_path: Path) -> None:
    """Template selection rules saved for a job_id should round-trip exactly."""
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")

    store.save_template_selection_rules(
        job_id=job_id,
        name="Example Job",
        selection_rules=TemplateSelectionRules(include=("a/**",), exclude=("b/**",)),
    )
    loaded = store.load_template_selection_rules(job_id)

    assert loaded == TemplateSelectionRules(include=("a/**",), exclude=("b/**",))


def test_template_selection_rules_are_authoritative(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

    store.save_template_selection_rules(
        job_id=job_id,
        name="Example Job",
        selection_rules=TemplateSelectionRules(include=("a/**",), exclude=("b/**",)),
    )

    loaded = store.load_template_selection_rules(job_id)
    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        template_rows = connection.execute(
            "SELECT kind, pattern, position FROM template_selection_rules "
            "WHERE template_id = ? ORDER BY kind ASC, position ASC",
            (binding.template_id,),
        ).fetchall()
    finally:
        connection.close()

    assert loaded == TemplateSelectionRules(include=("a/**",), exclude=("b/**",))
    assert [(str(row["kind"]), str(row["pattern"])) for row in template_rows] == [
        ("exclude", "b/**"),
        ("include", "a/**"),
    ]


def test_template_compression_is_authoritative(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

    store.save_template_compression(job_id=job_id, name="Example Job", compression="zip")
    loaded_compression = store.load_template_compression(job_id)

    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        template_row = connection.execute(
            "SELECT compression FROM template_backup_policy WHERE template_id = ?",
            (binding.template_id,),
        ).fetchone()
    finally:
        connection.close()

    assert loaded_compression == "zip"
    assert template_row is not None
    assert str(template_row["compression"]) == "zip"


def test_create_job_initializes_binding_without_template_policy(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    job_id = store.create_job("Example Job")
    binding = store.load_job_binding(job_id)

    assert binding.job_id == job_id
    assert binding.job_name == "Example Job"
    assert binding.template_id
    assert binding.template_id != job_id
    assert binding.source_root == ""


def test_save_job_binding_updates_name_template_and_source_root(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    original_job_id = store.create_job("Original Job")
    reusable_template_job_id = store.create_job("Reusable Template Job")
    reusable_binding = store.load_job_binding(reusable_template_job_id)

    store.save_job_binding(
        JobBinding(
            job_id=original_job_id,
            job_name="Updated Job",
            template_id=reusable_binding.template_id,
            source_root="C:/games/world",
        )
    )

    updated_binding = store.load_job_binding(original_job_id)
    assert updated_binding.job_name == "Updated Job"
    assert updated_binding.template_id == reusable_binding.template_id
    assert updated_binding.source_root == "C:/games/world"


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


def test_legacy_database_reports_clear_incompatibility_message(tmp_path: Path) -> None:
    database_path = tmp_path / "profiles.sqlite"
    store = SqliteProfileStore(db_path=database_path)
    job_id = store.create_job("Example Job")

    connection = sqlite3.connect(database_path)
    try:
        connection.execute("UPDATE jobs SET template_id = NULL WHERE job_id = ?", (job_id,))
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(IncompatibleProfileStoreError) as exc_info:
        store.load_job_binding(job_id)

    message = str(exc_info.value)
    assert "pre-contract WCBT build" in message
    assert "template_id" in message
    assert "Delete and recreate the local profile database" in message
    assert "future migration tool" in message


def test_validate_profile_store_contract_reports_legacy_database(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    database_path = profile_store_db_path(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("Example Job")

    connection = sqlite3.connect(database_path)
    try:
        connection.execute("UPDATE jobs SET template_id = '' WHERE job_id = ?", (job_id,))
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(IncompatibleProfileStoreError) as exc_info:
        validate_profile_store_contract(profile_name="default", data_root=tmp_path)

    assert "pre-contract WCBT build" in str(exc_info.value)


def test_mirror_tables_are_removed_from_schema(tmp_path: Path) -> None:
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")
    _ = store.create_job("Example Job")

    connection = sqlite3.connect(tmp_path / "profiles.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        table_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC"
        ).fetchall()
    finally:
        connection.close()

    table_names = {str(row["name"]) for row in table_rows}
    assert "rules" not in table_names
    assert "job_backup_defaults" not in table_names
    assert "scheduled_backup_legacy_inputs" not in table_names
