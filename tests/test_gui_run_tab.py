from __future__ import annotations

from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QMetaObject, QObject, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QApplication, QMessageBox

from backup_engine.backup.service import BackupRunResult
from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.api import JobSummary
from backup_engine.profile_store.sqlite_store import open_profile_store
from gui.settings_store import GuiSettings
from gui.tabs.run_tab import BackupWorker, RunTab


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


def test_backup_worker_plan_mode_uses_oz0_artifact_root_in_report_text(tmp_path: Path) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "level.dat").write_text("alpha", encoding="utf-8")

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    worker = BackupWorker(profile_name="default", data_root=data_root)
    finished_results: list[object] = []
    failed_messages: list[str] = []
    worker.finished.connect(finished_results.append)
    worker.failed.connect(failed_messages.append)

    worker.configure(
        job_id,
        "Minecraft",
        source_root,
        "plan",
        data_root=data_root,
        default_compression="none",
    )
    worker.run()

    assert failed_messages == []
    assert len(finished_results) == 1

    result = cast(BackupRunResult, finished_results[0])
    report_lines = result.report_text.splitlines()
    expected_oz0_root = source_root.parent / "testing.OZ0"

    assert f"Source root : {source_root}" in report_lines
    assert "Backup origin: Normal backup" in report_lines
    assert f"Artifact root: {expected_oz0_root}" in report_lines
    assert all(not line.startswith("Archive root:") for line in report_lines)
    assert result.archive_root == expected_oz0_root


class _FakeProfileStoreAdapter(QObject):
    request_list_jobs = Signal()

    jobs_loaded = Signal(object)
    error = Signal(str, str)

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__()
        _ = profile_name
        _ = data_root

    def shutdown(self) -> None:
        pass


def test_run_tab_persists_last_selected_run_mode_across_reopen(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    monkeypatch.setattr("gui.settings_store.default_data_root", lambda: tmp_path)
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)

    first_tab = RunTab()
    try:
        for index in range(first_tab.mode_combo.count()):
            if str(first_tab.mode_combo.itemData(index)) == "execute+compress":
                first_tab.mode_combo.setCurrentIndex(index)
                break
    finally:
        first_tab.shutdown()

    reopened_tab = RunTab()
    try:
        assert str(reopened_tab.mode_combo.currentData()) == "execute+compress"
    finally:
        reopened_tab.shutdown()


def test_run_tab_plan_mode_visible_summary_and_open_folder_use_oz0_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "level.dat").write_text("alpha", encoding="utf-8")

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="zip",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr("gui.tabs.run_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)

    tab = RunTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        worker = tab._worker  # noqa: SLF001
        worker.configure(
            job_id,
            "Minecraft",
            source_root,
            "plan",
            data_root=data_root,
            default_compression="zip",
        )
        worker.run()

        summary_text = tab.summary.toPlainText()
        expected_oz0_root = source_root.parent / "testing.OZ0"
        legacy_archives_root = str(data_root / "profiles" / "default" / "archives")

        assert "Backup origin: Normal backup" in summary_text
        assert f"Artifact root: {expected_oz0_root}" in summary_text
        assert legacy_archives_root not in summary_text
        assert tab._last_result is not None  # noqa: SLF001
        assert tab._last_result.archive_root == expected_oz0_root  # noqa: SLF001

        tab._open_artifacts()  # noqa: SLF001

        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == expected_oz0_root
    finally:
        tab.shutdown()


def test_run_tab_backup_now_refreshes_live_settings_for_oz0_plan_mode(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "level.dat").write_text("alpha", encoding="utf-8")

    current_settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr(
        "gui.tabs.run_tab.load_gui_settings",
        lambda *, data_root: current_settings,
    )
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _run_immediately(worker: object, member: str, *_args: object, **_kwargs: object) -> bool:
        assert member == "run"
        getattr(worker, member)()
        return True

    monkeypatch.setattr(QMetaObject, "invokeMethod", _run_immediately)

    tab = RunTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        current_settings = GuiSettings(
            data_root=data_root,
            archives_root=None,
            default_compression="zip",
            default_run_mode="plan",
        )

        tab._backup_now()  # noqa: SLF001

        expected_oz0_root = source_root.parent / "testing.OZ0"
        legacy_archives_root = str(data_root / "profiles" / "default" / "archives")
        summary_text = tab.summary.toPlainText()

        assert f"Artifact root: {expected_oz0_root}" in summary_text
        assert legacy_archives_root not in summary_text
        assert tab._last_result is not None  # noqa: SLF001
        assert tab._last_result.archive_root == expected_oz0_root  # noqa: SLF001

        tab._open_artifacts()  # noqa: SLF001

        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == expected_oz0_root
    finally:
        tab.shutdown()


def test_run_tab_plan_only_without_compression_uses_oz0_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "level.dat").write_text("alpha", encoding="utf-8")

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr("gui.tabs.run_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _run_immediately(worker: object, member: str, *_args: object, **_kwargs: object) -> bool:
        assert member == "run"
        getattr(worker, member)()
        return True

    monkeypatch.setattr(QMetaObject, "invokeMethod", _run_immediately)

    tab = RunTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab._backup_now()  # noqa: SLF001

        expected_oz0_root = source_root.parent / "testing.OZ0"
        legacy_archives_root = str(data_root / "profiles" / "default" / "archives")
        summary_text = tab.summary.toPlainText()

        assert f"Archive root: {expected_oz0_root}" in summary_text
        assert legacy_archives_root not in summary_text
        assert tab._last_result is not None  # noqa: SLF001
        assert tab._last_result.archive_root == expected_oz0_root  # noqa: SLF001
        assert len(list(expected_oz0_root.glob("plan_*.txt"))) == 1

        tab._open_artifacts()  # noqa: SLF001

        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == expected_oz0_root
    finally:
        tab.shutdown()


def test_run_tab_backup_now_plan_only_keeps_oz0_root_out_of_profile_archives(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "level.dat").write_text("alpha", encoding="utf-8")

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    monkeypatch.setattr("gui.tabs.run_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _run_immediately(worker: object, member: str, *_args: object, **_kwargs: object) -> bool:
        assert member == "run"
        getattr(worker, member)()
        return True

    monkeypatch.setattr(QMetaObject, "invokeMethod", _run_immediately)

    tab = RunTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab._backup_now()  # noqa: SLF001

        expected_oz0_root = source_root.parent / "testing.OZ0"
        legacy_archives_root = str(data_root / "profiles" / "default" / "archives")
        summary_text = tab.summary.toPlainText()

        assert str(expected_oz0_root) in summary_text
        assert legacy_archives_root not in summary_text
        assert tab._last_result is not None  # noqa: SLF001
        assert tab._last_result.archive_root == expected_oz0_root  # noqa: SLF001
        assert tab._last_result.plan_text_path is not None  # noqa: SLF001
        assert tab._last_result.plan_text_path.parent == expected_oz0_root  # noqa: SLF001
    finally:
        tab.shutdown()


def test_run_tab_open_artifacts_uses_job_binding_without_current_session_backup(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )

    opened_urls: list[QUrl] = []

    monkeypatch.setattr("gui.tabs.run_tab.load_gui_settings", lambda *, data_root: settings)
    monkeypatch.setattr("gui.tabs.run_tab.ProfileStoreAdapter", _FakeProfileStoreAdapter)
    monkeypatch.setattr(QDesktopServices, "openUrl", opened_urls.append)

    tab = RunTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        expected_oz0_root = source_root.parent / "testing.OZ0"

        assert tab._last_result is None  # noqa: SLF001
        assert not expected_oz0_root.exists()

        tab._open_artifacts()  # noqa: SLF001

        assert expected_oz0_root.is_dir()
        assert len(opened_urls) == 1
        assert Path(opened_urls[0].toLocalFile()) == expected_oz0_root
    finally:
        tab.shutdown()


def test_backup_worker_passes_manual_backup_note_to_run_backup(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    recorded_kwargs: dict[str, object] = {}

    def _run_backup(**kwargs: object) -> BackupRunResult:
        recorded_kwargs.update(kwargs)
        return BackupRunResult(
            run_id="run-id",
            profile_name="default",
            source_root=tmp_path / "world",
            archive_root=tmp_path / "world.OZ0",
            dry_run=True,
            report_text="ok",
            plan_text_path=None,
            manifest_path=None,
            executed=False,
            backup_note=str(kwargs.get("backup_note")) if kwargs.get("backup_note") else None,
        )

    monkeypatch.setattr("gui.tabs.run_tab.run_backup", _run_backup)
    monkeypatch.setattr(BackupWorker, "_resolve_plan_compression", lambda self, job_id: "none")

    worker = BackupWorker(profile_name="default", data_root=tmp_path / "data_root")
    (tmp_path / "world").mkdir()
    worker.configure(
        "job-1",
        "Minecraft",
        tmp_path / "world",
        "plan",
        "Before major mod update",
        data_root=tmp_path / "data_root",
        default_compression="none",
    )
    worker.run()

    assert recorded_kwargs["backup_note"] == "Before major mod update"
