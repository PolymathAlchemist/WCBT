from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QObject, Qt, Signal
from PySide6.QtWidgets import QApplication

from backup_engine.profile_store.api import JobSummary
from gui.tabs.restore_tab import RestoreTab


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


def test_restore_history_filters_new_runs_by_job_id_and_keeps_legacy_visible(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

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


def test_restore_history_all_history_keeps_missing_job_manifests_visible(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.tabs.restore_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

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
