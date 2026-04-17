from __future__ import annotations

from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtWidgets import QApplication, QWidget

from gui.app import AppWindow


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


class _FakeRunTab(QWidget):
    def shutdown(self) -> None:
        pass


class _FakeRestoreTab(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_count = 0

    def refresh_on_activate(self) -> None:
        self.refresh_count += 1

    def shutdown(self) -> None:
        pass


class _FakeAuthoringTab(QWidget):
    def shutdown(self) -> None:
        pass


class _FakeSchedulingTab(QWidget):
    def shutdown(self) -> None:
        pass


class _FakeSettingsTab(QWidget):
    pass


def test_app_window_refreshes_restore_tab_when_restore_tab_becomes_active(
    monkeypatch: MonkeyPatch,
) -> None:
    _app()

    monkeypatch.setattr("gui.app.RunTab", _FakeRunTab)
    monkeypatch.setattr("gui.app.RestoreTab", _FakeRestoreTab)
    monkeypatch.setattr("gui.app.AuthoringTab", _FakeAuthoringTab)
    monkeypatch.setattr("gui.app.SchedulingTab", _FakeSchedulingTab)
    monkeypatch.setattr("gui.app.SettingsTab", _FakeSettingsTab)

    window = AppWindow()
    try:
        restore_tab = cast(_FakeRestoreTab, window.restore_tab)
        assert restore_tab.refresh_count == 0

        window.tabs.setCurrentIndex(1)

        assert restore_tab.refresh_count == 1

        window.tabs.setCurrentIndex(0)
        window.tabs.setCurrentIndex(1)

        assert restore_tab.refresh_count == 2
    finally:
        window.close()
