from __future__ import annotations

from argparse import ArgumentParser
from typing import Sequence

import pytest

from wcbt.cli import _build_parser


def _parse(parser: ArgumentParser, argv: Sequence[str]) -> object:
    return parser.parse_args(list(argv))


def test_restore_parser_exposes_expected_choices_and_defaults() -> None:
    parser = _build_parser()

    # Minimal valid restore invocation (should parse without executing anything).
    args = _parse(
        parser,
        [
            "restore",
            "--manifest",
            "C:/tmp/manifest.json",
            "--dest",
            "C:/tmp/dest",
        ],
    )

    assert getattr(args, "command") == "restore"
    assert getattr(args, "mode") == "add-only"
    assert getattr(args, "verify") == "size"
    assert getattr(args, "dry_run") is False


@pytest.mark.parametrize("mode", ["add-only", "overwrite"])
def test_restore_mode_accepts_supported_values(mode: str) -> None:
    parser = _build_parser()
    args = _parse(
        parser,
        [
            "restore",
            "--manifest",
            "C:/tmp/manifest.json",
            "--dest",
            "C:/tmp/dest",
            "--mode",
            mode,
        ],
    )
    assert getattr(args, "mode") == mode


@pytest.mark.parametrize("verify", ["none", "size"])
def test_restore_verify_accepts_supported_values(verify: str) -> None:
    parser = _build_parser()
    args = _parse(
        parser,
        [
            "restore",
            "--manifest",
            "C:/tmp/manifest.json",
            "--dest",
            "C:/tmp/dest",
            "--verify",
            verify,
        ],
    )
    assert getattr(args, "verify") == verify
