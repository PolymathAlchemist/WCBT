from __future__ import annotations

from pathlib import Path

from backup_engine.backup.service import run_backup
from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.verify import verify_run


def _write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _single_run_id(artifact_root: Path) -> str:
    run_dirs = sorted([p for p in artifact_root.iterdir() if p.is_dir()])
    assert len(run_dirs) == 1, f"Expected exactly 1 run directory, found {len(run_dirs)}."
    return run_dirs[0].name


def _create_job_binding(
    *, profile_name: str, data_root: Path, source_root: Path
) -> tuple[str, str]:
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    job_name = "Minecraft"
    job_id = store.create_job(job_name)
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    return job_id, job_name


def test_verify_run_writes_report_on_success(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)

    _write_file(source_root / "a.txt", b"alpha\n")
    _write_file(source_root / "nested" / "b.bin", b"\x00\x01\x02")

    profile_name = "test-profile"
    job_id, job_name = _create_job_binding(
        profile_name=profile_name,
        data_root=data_root,
        source_root=source_root,
    )

    run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=data_root,
        execute=True,
        force=True,
        break_lock=True,
        job_id=job_id,
        job_name=job_name,
    )

    artifact_root = source_root.parent / "source.OZ0"
    run_id = _single_run_id(artifact_root)
    run_root = artifact_root / run_id

    verify_run(profile_name=profile_name, run_id=run_id, data_root=data_root)

    assert (run_root / "verify_report.json").exists()
    assert (run_root / "verify_summary.txt").exists()
