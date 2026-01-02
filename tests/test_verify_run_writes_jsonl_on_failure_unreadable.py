from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import backup_engine.verify as verify_module
from backup_engine.backup.service import run_backup
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.verify import VerifyError, verify_run


def _write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _single_run_id(archives_root: Path) -> str:
    run_dirs = sorted(p for p in archives_root.iterdir() if p.is_dir())
    assert len(run_dirs) == 1
    return run_dirs[0].name


def test_verify_run_writes_jsonl_on_failure_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "data"
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)

    _write_file(source_root / "a.txt", b"alpha\n")

    profile_name = "test-profile"

    run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=data_root,
        execute=True,
        force=True,
        break_lock=True,
    )

    paths = resolve_profile_paths(profile_name, data_root=data_root)
    run_id = _single_run_id(paths.archives_root)
    run_root = paths.archives_root / run_id

    # Locate a real payload file
    payload_files = [
        p
        for p in run_root.rglob("*")
        if p.is_file()
        and p.name not in {"manifest.json", "plan.txt"}
        and not p.name.startswith("verify_")
    ]
    assert payload_files
    target = payload_files[0]

    # Patch compute_digest to simulate unreadable file
    real_compute = verify_module.compute_digest

    def _raise_permission(path: Path, algorithm: Any) -> str:
        if path == target:
            raise PermissionError("simulated permission error")
        return real_compute(path, algorithm)

    monkeypatch.setattr(verify_module, "compute_digest", _raise_permission)

    with pytest.raises(VerifyError):
        verify_run(profile_name=profile_name, run_id=run_id, data_root=data_root)

    jsonl_path = run_root / "verify_report.jsonl"
    assert jsonl_path.exists()

    records = [json.loads(line) for line in jsonl_path.read_text().splitlines() if line.strip()]
    assert any(r["status"] == "unreadable" for r in records)
