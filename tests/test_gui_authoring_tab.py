from __future__ import annotations

from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from gui.settings_store import GuiSettings
from gui.tabs.authoring_tab import AuthoringTab


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


class _CapturingProfileStoreAdapter(QObject):
    request_load_rules = Signal(str)
    request_save_rules = Signal(str, str, object)
    request_list_jobs = Signal()
    request_create_job = Signal(str)
    request_rename_job = Signal(str, str)
    request_delete_job = Signal(str)

    rules_loaded = Signal(str, object)
    rules_saved = Signal(str)
    unknown_job = Signal(str)
    error = Signal(str, str)
    jobs_loaded = Signal(object)
    job_created = Signal(str)
    job_renamed = Signal(str)
    job_deleted = Signal(str)

    last_data_root: Path | None = None

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__()
        _ = profile_name
        type(self).last_data_root = data_root

    def shutdown(self) -> None:
        pass


def test_authoring_tab_uses_gui_settings_data_root_for_profile_store(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    _CapturingProfileStoreAdapter.last_data_root = None

    monkeypatch.setattr(
        "gui.tabs.authoring_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.authoring_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )

    tab = AuthoringTab()
    try:
        assert _CapturingProfileStoreAdapter.last_data_root == data_root
    finally:
        tab.shutdown()
