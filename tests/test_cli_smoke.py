"""
CLI smoke tests.

These tests intentionally validate only that the CLI entrypoint is wired and that
help output for the root command and key subcommands does not crash.
"""

from __future__ import annotations

import pytest

from wcbt.cli import main


def _run_help(argv: list[str]) -> None:
    """Run the CLI expecting argparse to exit cleanly with code 0 for --help."""
    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    assert excinfo.value.code == 0


def test_cli_root_help(capsys: pytest.CaptureFixture[str]) -> None:
    _run_help(["--help"])
    captured = capsys.readouterr()
    assert "usage:" in captured.out.lower()
    assert "wcbt" in captured.out.lower()


@pytest.mark.parametrize("subcommand", ["init-profile", "backup", "verify", "restore"])
def test_cli_subcommand_help(subcommand: str, capsys: pytest.CaptureFixture[str]) -> None:
    _run_help([subcommand, "--help"])
    captured = capsys.readouterr()
    out = captured.out.lower()
    assert "usage:" in out
    assert subcommand in out
