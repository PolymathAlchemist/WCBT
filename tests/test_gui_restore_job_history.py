from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QObject, Signal
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
        assert not any("run-job-b" in item for item in visible_for_a)
        assert any("run-legacy" in item for item in visible_for_a)

        tab.job_combo.setCurrentIndex(1)

        visible_for_b = [tab.history.item(i).text() for i in range(tab.history.count())]
        assert any("run-job-b" in item for item in visible_for_b)
        assert not any("run-job-a" in item for item in visible_for_b)
        assert any("run-legacy" in item for item in visible_for_b)
    finally:
        tab.shutdown()
