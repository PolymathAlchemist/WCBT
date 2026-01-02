from __future__ import annotations

from pathlib import Path

import pytest

import wcbt.cli as cli_module
from backup_engine.errors import WcbtError
from backup_engine.paths_and_safety import SafetyViolationError
from backup_engine.restore.errors import RestoreConflictError


def test_cli_backup_rejects_write_plan_with_execute(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli_module.main(
        [
            "backup",
            "--profile",
            "p",
            "--source",
            str(Path("C:/tmp/source")),
            "--execute",
            "--write-plan",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 2
    assert "only valid in plan-only mode" in out


def test_cli_backup_returns_2_on_wcbt_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _boom(**_kwargs: object) -> None:
        raise WcbtError("nope")

    monkeypatch.setattr(cli_module, "run_backup", _boom)

    rc = cli_module.main(["backup", "--profile", "p", "--source", str(Path("C:/tmp/source"))])
    out = capsys.readouterr().out
    assert rc == 2
    assert "ERROR:" in out


def test_cli_restore_returns_2_on_conflict(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _conflict(**_kwargs: object) -> None:
        raise RestoreConflictError("conflict")

    monkeypatch.setattr(cli_module, "run_restore", _conflict)

    rc = cli_module.main(
        [
            "restore",
            "--manifest",
            str(Path("C:/tmp/manifest.json")),
            "--dest",
            str(Path("C:/tmp/dest")),
        ]
    )
    out = capsys.readouterr().out
    assert rc == 2
    assert "ERROR:" in out


def test_cli_restore_returns_1_on_safety_or_domain_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _boom(**_kwargs: object) -> None:
        raise SafetyViolationError("unsafe")

    monkeypatch.setattr(cli_module, "run_restore", _boom)

    rc = cli_module.main(
        [
            "restore",
            "--manifest",
            str(Path("C:/tmp/manifest.json")),
            "--dest",
            str(Path("C:/tmp/dest")),
        ]
    )
    out = capsys.readouterr().out
    assert rc == 1
    assert "ERROR:" in out


def test_cli_verify_returns_2_on_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # verify_run is imported inside the verify branch, so we patch the function in that module.
    import backup_engine.verify as verify_module

    def _boom(**_kwargs: object) -> None:
        raise WcbtError("verify failed")

    monkeypatch.setattr(verify_module, "verify_run", _boom)

    rc = cli_module.main(["verify", "--profile", "p", "--run-id", "RID"])
    out = capsys.readouterr().out
    assert rc == 2
    assert "ERROR:" in out


def test_cli_init_profile_prints_paths_when_requested(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Paths:
        pass

    def _init_profile(*, profile_name: str, data_root: Path | None) -> _Paths:
        assert profile_name == "p"
        assert data_root is None
        return _Paths()

    def _paths_as_text(_paths: _Paths) -> str:
        return "paths!"

    monkeypatch.setattr(cli_module, "init_profile", _init_profile)
    monkeypatch.setattr(cli_module, "profile_paths_as_text", _paths_as_text)

    rc = cli_module.main(["init-profile", "--profile", "p", "--print-paths"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "paths!" in out
