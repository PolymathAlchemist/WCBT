from __future__ import annotations

from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtWidgets import QApplication, QMessageBox

from gui.settings_store import GuiSettings, load_gui_settings, save_gui_settings
from gui.tabs.settings_tab import SettingsTab


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


def test_gui_settings_defaults_enable_compressed_pre_restore_backup() -> None:
    settings = GuiSettings.defaults()

    assert settings.pre_restore_backup_compression == "zip"


def test_gui_settings_round_trip_pre_restore_backup_compression(tmp_path: Path) -> None:
    settings = GuiSettings(
        data_root=tmp_path / "data_root",
        archives_root=tmp_path / "archives_root",
        default_compression="none",
        default_run_mode="plan",
        restore_mode="overwrite",
        restore_verify="none",
        restore_dry_run=False,
        pre_restore_backup_compression="tar.zst",
    )

    save_gui_settings(data_root=tmp_path, settings=settings)
    loaded = load_gui_settings(data_root=tmp_path)

    assert loaded.pre_restore_backup_compression == "tar.zst"


def test_settings_tab_saves_pre_restore_backup_compression(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    saved_settings: list[GuiSettings] = []

    monkeypatch.setattr(
        "gui.tabs.settings_tab.load_gui_settings",
        lambda *, data_root: GuiSettings(
            data_root=tmp_path / "data_root",
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
            restore_mode="add-only",
            restore_verify="size",
            restore_dry_run=True,
            pre_restore_backup_compression="zip",
        ),
    )
    monkeypatch.setattr(
        "gui.tabs.settings_tab.save_gui_settings",
        lambda *, data_root, settings: saved_settings.append(settings),
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)

    tab = SettingsTab()
    try:
        for index in range(tab.pre_restore_compression_combo.count()):
            if str(tab.pre_restore_compression_combo.itemData(index)) == "none":
                tab.pre_restore_compression_combo.setCurrentIndex(index)
                break

        tab._save()  # noqa: SLF001

        assert len(saved_settings) == 1
        assert saved_settings[0].pre_restore_backup_compression == "none"
    finally:
        tab.deleteLater()
