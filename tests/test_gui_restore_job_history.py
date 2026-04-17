from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QObject, Qt, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QApplication, QMessageBox, QPushButton

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.api import JobSummary
from backup_engine.profile_store.sqlite_store import open_profile_store
from gui.settings_store import GuiSettings
from gui.tabs.restore_tab import RestoreTab, RestoreWorker


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class _FakeProfileStoreAdapter(QObject):
    request_list_jobs = Signal()
    request_load_restore_defaults = Signal(str)
    request_save_restore_defaults = Signal(str, object)

    jobs_loaded = Signal(object)
    error = Signal(str, str)
    restore_defaults_loaded = Signal(str, object)
    restore_defaults_saved = Signal(str)

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__()
        _ = profile_name
        _ = data_root

    def shutdown(self) -> None:
        pass


class _CapturingProfileStoreAdapter(_FakeProfileStoreAdapter):
    last_data_root: Path | None = None

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__(profile_name=profile_name, data_root=data_root)
        type(self).last_data_root = data_root


def test_restore_tab_persists_last_selected_restore_preferences_across_reopen(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.settings_store.default_data_root", lambda: tmp_path)
    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    first_tab = RestoreTab()
    try:
        for index in range(first_tab.mode_combo.count()):
            if str(first_tab.mode_combo.itemData(index)) == "overwrite":
                first_tab.mode_combo.setCurrentIndex(index)
                break
        for index in range(first_tab.verify_combo.count()):
            if str(first_tab.verify_combo.itemData(index)) == "none":
                first_tab.verify_combo.setCurrentIndex(index)
                break
        first_tab.dry_run.setChecked(False)
    finally:
        first_tab.shutdown()

    reopened_tab = RestoreTab()
    try:
        assert str(reopened_tab.mode_combo.currentData()) == "overwrite"
        assert str(reopened_tab.verify_combo.currentData()) == "none"
        assert reopened_tab.dry_run.isChecked() is False
    finally:
        reopened_tab.shutdown()


def test_restore_tab_persists_last_selected_job_across_reopen(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.settings_store.default_data_root", lambda: tmp_path)
    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    first_tab = RestoreTab()
    try:
        first_tab._on_jobs_loaded(  # noqa: SLF001
            [
                JobSummary(job_id="job-a", name="Job A"),
                JobSummary(job_id="job-b", name="Job B"),
            ]
        )
        first_tab.job_combo.setCurrentIndex(2)
        assert str(first_tab.job_combo.currentData()) == "job-b"
    finally:
        first_tab.shutdown()

    reopened_tab = RestoreTab()
    try:
        reopened_tab._on_jobs_loaded(  # noqa: SLF001
            [
                JobSummary(job_id="job-a", name="Job A"),
                JobSummary(job_id="job-b", name="Job B"),
            ]
        )
        assert str(reopened_tab.job_combo.currentData()) == "job-b"
    finally:
        reopened_tab.shutdown()


def test_restore_history_filters_new_runs_by_job_id_and_keeps_legacy_visible(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(
        "gui.tabs.restore_tab.load_gui_settings",
        lambda *, data_root: GuiSettings(
            data_root=tmp_path / "data_root",
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
        ),
    )

    archive_root = tmp_path / "archive"

    _write_manifest(
        archive_root / "run-job-a" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-job-a",
            "created_at_utc": "2026-01-01T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-job-a" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "backup_origin": "normal",
            "job_id": "job-a-id",
            "job_name": "Job A",
            "operations": [],
            "scan_issues": [],
        },
    )
    _write_manifest(
        archive_root / "run-job-b" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-job-b",
            "created_at_utc": "2026-01-02T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-job-b" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "backup_origin": "scheduled",
            "job_id": "job-b-id",
            "job_name": "Job B",
            "operations": [],
            "scan_issues": [],
        },
    )
    _write_manifest(
        archive_root / "run-legacy" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-legacy",
            "created_at_utc": "2025-12-31T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-legacy" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded(  # noqa: SLF001
            [
                JobSummary(job_id="job-a-id", name="Job A"),
                JobSummary(job_id="job-b-id", name="Job B"),
            ]
        )
        tab.archive_root.setText(str(archive_root))

        visible_for_a = [tab.history.item(i).text() for i in range(tab.history.count())]
        assert any("run-job-a" in item for item in visible_for_a)
        assert any("[Normal backup]" in item for item in visible_for_a)
        assert not any("run-job-b" in item for item in visible_for_a)
        assert any("run-legacy" in item for item in visible_for_a)

        tab.job_combo.setCurrentIndex(2)

        visible_for_b = [tab.history.item(i).text() for i in range(tab.history.count())]
        assert any("run-job-b" in item for item in visible_for_b)
        assert any("[Scheduled backup]" in item for item in visible_for_b)
        assert not any("run-job-a" in item for item in visible_for_b)
        assert any("run-legacy" in item for item in visible_for_b)
    finally:
        tab.shutdown()


def test_restore_history_with_zero_active_jobs_still_populates_from_manifests(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    manifest_path = archive_root / "run-orphaned" / "manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-orphaned",
            "created_at_utc": "2026-01-03T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-orphaned" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "job_id": "deleted-job-id",
            "job_name": "Deleted Job",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))

        assert tab.job_combo.count() == 1
        assert tab.job_combo.itemText(0) == "All history"
        assert tab.history.count() == 1
        assert "run-orphaned" in tab.history.item(0).text()
        assert tab.btn_restore.isEnabled()
    finally:
        tab.shutdown()


def test_restore_defaults_loaded_repopulates_history_without_manual_archive_root_change(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(
        "gui.tabs.restore_tab.load_gui_settings",
        lambda *, data_root: GuiSettings(
            data_root=tmp_path / "data_root",
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
        ),
    )

    archive_root = tmp_path / "archive"
    _write_manifest(
        archive_root / "run-job-a" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-job-a",
            "created_at_utc": "2026-01-04T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-job-a" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "job_id": "job-a-id",
            "job_name": "Job A",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id="job-a-id", name="Job A")])  # noqa: SLF001

        assert tab.history.count() == 0

        tab._on_restore_defaults_loaded(  # noqa: SLF001
            "job-a-id",
            {
                "archive_root": str(archive_root),
                "restore_dest_root": str(tmp_path / "restore-destination"),
            },
        )

        assert tab.history.count() == 1
        assert "run-job-a" in tab.history.item(0).text()
    finally:
        tab.shutdown()


def test_restore_tab_uses_persisted_gui_settings_data_root_on_reopen(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _CapturingProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        assert _CapturingProfileStoreAdapter.last_data_root == data_root
        assert tab._worker._data_root == data_root  # noqa: SLF001
    finally:
        tab.shutdown()


def test_restore_history_auto_discovers_existing_oz0_manifests_from_saved_job_binding(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    oz0_root = source_root.parent / "world.OZ0"
    manifest_path = oz0_root / "Minecraft.20260108_000000Z.manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "20260108_000000Z",
            "created_at_utc": "2026-01-08T00:00:00Z",
            "archive_root": str(oz0_root),
            "plan_text_path": str(oz0_root / "plan_20260108_000000Z.txt"),
            "profile_name": "default",
            "source_root": str(source_root),
            "job_id": job_id,
            "job_name": "Minecraft",
            "operations": [],
            "scan_issues": [],
        },
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        assert tab.archive_root.text() == str(oz0_root)
        assert tab.history.count() == 1
        assert "20260108_000000Z" in tab.history.item(0).text()
        assert Path(str(tab.history.item(0).data(Qt.ItemDataRole.UserRole))) == manifest_path
    finally:
        tab.shutdown()


def test_restore_history_falls_back_to_job_root_when_persisted_archive_root_is_stale(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    oz0_root = source_root.parent / "world.OZ0"
    manifest_path = oz0_root / "Minecraft.20260108_000000Z.manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "20260108_000000Z",
            "created_at_utc": "2026-01-08T00:00:00Z",
            "archive_root": str(oz0_root),
            "plan_text_path": str(oz0_root / "plan_20260108_000000Z.txt"),
            "profile_name": "default",
            "source_root": str(source_root),
            "job_id": job_id,
            "job_name": "Minecraft",
            "operations": [],
            "scan_issues": [],
        },
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab.archive_root.setText(str(tmp_path / "missing-archive-root"))

        assert tab.history.count() == 1
        assert "20260108_000000Z" in tab.history.item(0).text()
        assert Path(str(tab.history.item(0).data(Qt.ItemDataRole.UserRole))) == manifest_path
    finally:
        tab.shutdown()


def test_restore_history_displays_authoritative_root_instead_of_stale_legacy_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab._on_restore_defaults_loaded(  # noqa: SLF001
            job_id,
            {
                "archive_root": str(source_root.parent / "OZ0"),
                "restore_dest_root": str(tmp_path / "restore-destination"),
            },
        )

        assert tab.archive_root_label.text() == "Artifacts root:"
        assert tab.archive_root.text() == str(source_root.parent / "testing.OZ0")
    finally:
        tab.shutdown()


def test_restore_history_marks_field_as_manual_override_when_text_differs_from_authoritative_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab.archive_root.setText(str(tmp_path / "manual-override"))

        assert tab.archive_root_label.text() == "History root override:"
    finally:
        tab.shutdown()


def test_restore_history_refresh_on_activate_rediscovers_new_manifest(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        assert tab.history.count() == 0

        oz0_root = source_root.parent / "world.OZ0"
        manifest_path = oz0_root / "Minecraft.20260108_000000Z.manifest.json"
        _write_manifest(
            manifest_path,
            {
                "schema_version": "wcbt_run_manifest_v2",
                "run_id": "20260108_000000Z",
                "created_at_utc": "2026-01-08T00:00:00Z",
                "archive_root": str(oz0_root),
                "plan_text_path": str(oz0_root / "plan_20260108_000000Z.txt"),
                "profile_name": "default",
                "source_root": str(source_root),
                "job_id": job_id,
                "job_name": "Minecraft",
                "operations": [],
                "scan_issues": [],
            },
        )

        tab.refresh_on_activate()

        assert tab.history.count() == 1
        assert Path(str(tab.history.item(0).data(Qt.ItemDataRole.UserRole))) == manifest_path
    finally:
        tab.shutdown()


def test_restore_history_auto_discovers_legacy_oz0_manifests_from_saved_job_binding(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    legacy_oz0_root = source_root.parent / "OZ0"
    manifest_path = legacy_oz0_root / "Minecraft.20260108_000000Z.manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "20260108_000000Z",
            "created_at_utc": "2026-01-08T00:00:00Z",
            "archive_root": str(legacy_oz0_root),
            "plan_text_path": str(legacy_oz0_root / "plan_20260108_000000Z.txt"),
            "profile_name": "default",
            "source_root": str(source_root),
            "job_id": job_id,
            "job_name": "Minecraft",
            "operations": [],
            "scan_issues": [],
        },
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        assert tab.archive_root.text() == str(source_root.parent / "world.OZ0")
        assert tab.history.count() == 1
        assert "20260108_000000Z" in tab.history.item(0).text()
        assert Path(str(tab.history.item(0).data(Qt.ItemDataRole.UserRole))) == manifest_path
    finally:
        tab.shutdown()


def test_restore_history_all_history_keeps_missing_job_manifests_visible(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(
        "gui.tabs.restore_tab.load_gui_settings",
        lambda *, data_root: GuiSettings(
            data_root=tmp_path / "data_root",
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
        ),
    )

    archive_root = tmp_path / "archive"

    _write_manifest(
        archive_root / "run-job-a" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-job-a",
            "created_at_utc": "2026-01-01T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-job-a" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "job_id": "job-a-id",
            "job_name": "Job A",
            "operations": [],
            "scan_issues": [],
        },
    )
    _write_manifest(
        archive_root / "run-missing-job" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-missing-job",
            "created_at_utc": "2026-01-04T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-missing-job" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "job_id": "missing-job-id",
            "job_name": "Missing Job",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id="job-a-id", name="Job A")])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))
        tab.job_combo.setCurrentIndex(0)

        visible_items = [tab.history.item(i).text() for i in range(tab.history.count())]
        current_item = tab.history.currentItem()

        assert any("run-job-a" in item for item in visible_items)
        assert any("run-missing-job" in item for item in visible_items)
        assert tab.btn_restore.isEnabled()
        assert current_item is not None
        assert tab._selected_manifest_path == Path(str(current_item.data(Qt.ItemDataRole.UserRole)))
    finally:
        tab.shutdown()


def test_restore_history_details_show_pre_restore_backup_origin(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    manifest_path = archive_root / "run-pre-restore" / "manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-pre-restore",
            "created_at_utc": "2026-01-05T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-pre-restore" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "backup_origin": "pre_restore",
            "backup_note": (
                "Pre-restore safety backup created before restoring run "
                "run-pre-restore over active files at C:/restore-destination"
            ),
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))

        history_text = tab.history.item(0).text()
        details_text = tab.details.toPlainText()

        assert "[Pre-restore safeguard backup]" in history_text
        assert "backup_origin: Pre-restore safeguard backup" in details_text
        assert (
            "backup_note: Pre-restore safety backup created before restoring run "
            "run-pre-restore over active files at C:/restore-destination"
        ) in details_text
    finally:
        tab.shutdown()


def test_restore_history_details_show_normal_backup_origin(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    manifest_path = archive_root / "run-normal" / "manifest.json"
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-normal",
            "created_at_utc": "2026-01-06T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-normal" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "backup_origin": "normal",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))

        history_text = tab.history.item(0).text()
        details_text = tab.details.toPlainText()

        assert "[Normal backup]" in history_text
        assert "backup_origin: Normal backup" in details_text
    finally:
        tab.shutdown()


def test_restore_selected_run_paths_stay_synced_after_reload_filter_and_selection(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    run_a_manifest = archive_root / "run-a" / "manifest.json"
    run_b_manifest = archive_root / "run-b" / "manifest.json"

    _write_manifest(
        run_a_manifest,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-a",
            "created_at_utc": "2026-01-05T00:00:00Z",
            "archive_root": str(archive_root / "artifacts-a"),
            "plan_text_path": str(archive_root / "run-a" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source-a",
            "job_id": "job-a-id",
            "job_name": "Job A",
            "operations": [],
            "scan_issues": [],
        },
    )
    _write_manifest(
        run_b_manifest,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-b",
            "created_at_utc": "2026-01-06T00:00:00Z",
            "archive_root": str(archive_root / "artifacts-b"),
            "plan_text_path": str(archive_root / "run-b" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source-b",
            "job_id": "job-b-id",
            "job_name": "Job B",
            "operations": [],
            "scan_issues": [],
        },
    )
    os.utime(run_a_manifest, (100, 100))
    os.utime(run_b_manifest, (200, 200))

    opened_urls: list[QUrl] = []
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)

    def _assert_selected_paths(tab: RestoreTab) -> None:
        current_item = tab.history.currentItem()
        assert current_item is not None
        selected_run = tab._selected_run_from_item(current_item)  # noqa: SLF001
        assert selected_run is not None
        assert tab._selected_manifest_path == selected_run.manifest_path  # noqa: SLF001
        assert tab._selected_run_summary == selected_run  # noqa: SLF001
        details_text = tab.details.toPlainText()
        assert f"  path: {selected_run.manifest_path}" in details_text
        assert f"  artifacts_root: {selected_run.archive_root}" in details_text

        before_count = len(opened_urls)
        tab._open_manifest_folder()  # noqa: SLF001
        tab._open_artifacts_root()  # noqa: SLF001

        assert len(opened_urls) == before_count + 2
        assert Path(opened_urls[-2].toLocalFile()) == selected_run.manifest_path.parent
        assert Path(opened_urls[-1].toLocalFile()) == Path(str(selected_run.archive_root))

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded(  # noqa: SLF001
            [
                JobSummary(job_id="job-a-id", name="Job A"),
                JobSummary(job_id="job-b-id", name="Job B"),
            ]
        )
        tab.archive_root.setText(str(archive_root))

        tab.refresh_on_activate()
        _assert_selected_paths(tab)

        tab.job_combo.setCurrentIndex(2)
        _assert_selected_paths(tab)

        tab.job_combo.setCurrentIndex(0)
        tab.history.setCurrentRow(1)
        _assert_selected_paths(tab)
    finally:
        tab.shutdown()


def test_restore_summary_shows_backup_origin_labels(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    manifests = [
        ("run-normal", "normal", "Normal backup"),
        ("run-scheduled", "scheduled", "Scheduled backup"),
        ("run-pre-restore", "pre_restore", "Pre-restore safeguard backup"),
    ]

    for run_id, backup_origin, _expected_label in manifests:
        _write_manifest(
            archive_root / run_id / "manifest.json",
            {
                "schema_version": "wcbt_run_manifest_v2",
                "run_id": run_id,
                "created_at_utc": "2026-01-07T00:00:00Z",
                "archive_root": str(archive_root),
                "plan_text_path": str(archive_root / run_id / "plan.txt"),
                "profile_name": "default",
                "source_root": "C:/source",
                "backup_origin": backup_origin,
                "operations": [],
                "scan_issues": [],
            },
        )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))
        tab.dest.setText(str(tmp_path / "restore-destination"))

        summary_by_run_id: dict[str, str] = {}
        for index in range(tab.history.count()):
            item = tab.history.item(index)
            tab.history.setCurrentItem(item)
            summary_by_run_id[item.text()] = tab._build_restore_summary()  # noqa: SLF001

        assert any(
            "backup_origin: Normal backup" in summary
            for history_text, summary in summary_by_run_id.items()
            if "run-normal" in history_text
        )
        assert any(
            "backup_origin: Scheduled backup" in summary
            for history_text, summary in summary_by_run_id.items()
            if "run-scheduled" in history_text
        )
        assert any(
            "backup_origin: Pre-restore safeguard backup" in summary
            for history_text, summary in summary_by_run_id.items()
            if "run-pre-restore" in history_text
        )
    finally:
        tab.shutdown()


def test_restore_history_filter_matches_backup_note_text(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    _write_manifest(
        archive_root / "run-scheduled" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-scheduled",
            "created_at_utc": "2026-01-07T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-scheduled" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "backup_origin": "scheduled",
            "backup_note": "Scheduled backup executed by Windows Task Scheduler",
            "operations": [],
            "scan_issues": [],
        },
    )
    _write_manifest(
        archive_root / "run-manual" / "manifest.json",
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run-manual",
            "created_at_utc": "2026-01-06T00:00:00Z",
            "archive_root": str(archive_root),
            "plan_text_path": str(archive_root / "run-manual" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "operations": [],
            "scan_issues": [],
        },
    )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))
        tab.filter_edit.setText("scheduler")

        assert tab.history.count() == 1
        assert "run-scheduled" in tab.history.item(0).text()
        assert (
            "backup_note: Scheduled backup executed by Windows Task Scheduler"
            in tab.details.toPlainText()
        )
    finally:
        tab.shutdown()


def test_restore_tab_keeps_single_copy_restore_summary_button_after_repeated_selections(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    archive_root = tmp_path / "archive"
    for run_id in ("run-a", "run-b", "run-c"):
        _write_manifest(
            archive_root / run_id / "manifest.json",
            {
                "schema_version": "wcbt_run_manifest_v2",
                "run_id": run_id,
                "created_at_utc": "2026-01-09T00:00:00Z",
                "archive_root": str(archive_root),
                "plan_text_path": str(archive_root / run_id / "plan.txt"),
                "profile_name": "default",
                "source_root": "C:/source",
                "operations": [],
                "scan_issues": [],
            },
        )

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([])  # noqa: SLF001
        tab.archive_root.setText(str(archive_root))

        for index in range(tab.history.count()):
            tab.history.setCurrentRow(index)

        matching_buttons = [
            button
            for button in tab.findChildren(QPushButton)
            if button.text() == "Copy Restore Summary"
        ]

        assert len(matching_buttons) == 1
        assert tab.btn_copy_summary is matching_buttons[0]  # noqa: SLF001
    finally:
        tab.shutdown()


def test_restore_tab_open_artifacts_root_uses_job_binding_without_session_restore(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        expected_root = source_root.parent / "world.OZ0"

        assert not expected_root.exists()
        tab._open_artifacts_root()  # noqa: SLF001

        assert expected_root.is_dir()
        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == expected_root
    finally:
        tab.shutdown()


def test_restore_tab_open_artifacts_root_prefers_job_binding_over_manual_legacy_archive_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "persisted-data-root"
    source_root = tmp_path / "world"
    source_root.mkdir()

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr("gui.tabs.restore_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)

    tab = RestoreTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        legacy_archive_root = source_root.parent / "OZ0"
        authoritative_root = source_root.parent / "world.OZ0"

        tab.archive_root.setText(str(legacy_archive_root))

        assert not authoritative_root.exists()
        tab._open_artifacts_root()  # noqa: SLF001

        assert authoritative_root.is_dir()
        assert not legacy_archive_root.exists()
        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == authoritative_root
    finally:
        tab.shutdown()


def test_restore_worker_passes_pre_restore_backup_compression_policy(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    recorded_kwargs: dict[str, object] = {}

    def _run_restore(**kwargs: object) -> object:
        recorded_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr("gui.tabs.restore_tab.run_restore", _run_restore)

    worker = RestoreWorker(data_root=tmp_path / "data_root")
    worker.configure(
        manifest_path=tmp_path / "manifest.json",
        destination_root=tmp_path / "restore-destination",
        mode="overwrite",
        verify="size",
        dry_run=False,
        pre_restore_backup_compression="tar.zst",
    )
    worker.run()

    assert recorded_kwargs["pre_restore_backup_compression"] == "tar.zst"


def test_restore_tab_housekeeping_removes_safe_transient_restore_residue(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(
        "gui.tabs.restore_tab.load_gui_settings",
        lambda *, data_root: GuiSettings(
            data_root=tmp_path / "data_root",
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
        ),
    )

    destination_root = tmp_path / "restore-destination"
    destination_root.mkdir()
    stage_root = destination_root.with_name(f"{destination_root.name}.wcbt_stage")
    extract_root = destination_root.with_name(f"{destination_root.name}.wcbt_restore_extract")
    previous_root = tmp_path / ".wcbt_restore_previous_restore-destination_20260109_000000Z"
    artifact_root = tmp_path / "keep-me.OZ0"
    manifest_path = artifact_root / "run" / "manifest.json"

    (stage_root / "123" / "stage_root").mkdir(parents=True)
    (extract_root / "123").mkdir(parents=True)
    previous_root.mkdir(parents=True)
    _write_manifest(
        manifest_path,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run",
            "created_at_utc": "2026-01-09T00:00:00Z",
            "archive_root": str(artifact_root),
            "plan_text_path": str(artifact_root / "run" / "plan.txt"),
            "profile_name": "default",
            "source_root": "C:/source",
            "operations": [],
            "scan_issues": [],
        },
    )

    info_messages: list[str] = []

    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda *args, **_kwargs: info_messages.append(str(args[2]) if len(args) > 2 else ""),
    )

    tab = RestoreTab()
    try:
        tab.dest.setText(str(destination_root))
        tab._run_housekeeping()  # noqa: SLF001

        assert not stage_root.exists()
        assert not extract_root.exists()
        assert not previous_root.exists()
        assert manifest_path.is_file()
        assert any("Removed 3 restore residue folder" in message for message in info_messages)
    finally:
        tab.shutdown()
