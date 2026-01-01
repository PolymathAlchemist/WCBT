from __future__ import annotations

from pathlib import Path

from backup_engine.backup.service import run_backup
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.verify import verify_run


def _write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _single_run_id(archives_root: Path) -> str:
    run_dirs = sorted([p for p in archives_root.iterdir() if p.is_dir()])
    assert len(run_dirs) == 1, f"Expected exactly 1 run directory, found {len(run_dirs)}."
    return run_dirs[0].name


def test_verify_run_writes_report_on_success(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)

    _write_file(source_root / "a.txt", b"alpha\n")
    _write_file(source_root / "nested" / "b.bin", b"\x00\x01\x02")

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

    verify_run(profile_name=profile_name, run_id=run_id, data_root=data_root)

    assert (run_root / "verify_report.json").exists()
    assert (run_root / "verify_summary.txt").exists()
