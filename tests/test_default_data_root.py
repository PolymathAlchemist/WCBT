from __future__ import annotations

from pathlib import Path

import pytest

from backup_engine.paths_and_safety import default_data_root


@pytest.mark.usefixtures("monkeypatch")
def test_default_data_root_prefers_local_appdata(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))

    root = default_data_root()
    assert root == (tmp_path / "Local" / "WCBT")


@pytest.mark.usefixtures("monkeypatch")
def test_default_data_root_falls_back_to_roaming(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))

    root = default_data_root()
    assert root == (tmp_path / "Roaming" / "WCBT")
