from pathlib import Path

import pytest

from backup_engine.restore.verify import RestoreVerificationError, verify_restore_stage


class DummyCandidate:
    def __init__(self, source_path: Path, relative_path: Path) -> None:
        self._source_path = source_path
        self._relative_path = relative_path

    def to_dict(self) -> dict[str, object]:
        return {
            "source_path": str(self._source_path),
            "relative_path": str(self._relative_path),
        }


def test_verify_stage_none_mode_accepts(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_file = source_root / "a.txt"
    source_file.write_bytes(b"x")

    stage_root = tmp_path / "stage_root"
    stage_root.mkdir()
    (stage_root / "a.txt").write_bytes(b"x")

    candidates = [DummyCandidate(source_file, Path("a.txt"))]
    result = verify_restore_stage(
        candidates=candidates,
        stage_root=stage_root,
        verification_mode="size",
        dry_run=False,
        journal=None,
    )
    assert result.verified_files == 1
    assert result.planned_files == 1


def test_verify_stage_size_mode_passes(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_file = source_root / "a.txt"
    source_file.write_bytes(b"x")

    stage_root = tmp_path / "stage_root"
    stage_root.mkdir()
    (stage_root / "a.txt").write_bytes(b"x")

    candidates = [DummyCandidate(source_file, Path("a.txt"))]
    result = verify_restore_stage(
        candidates=candidates,
        stage_root=stage_root,
        verification_mode="size",
        dry_run=False,
        journal=None,
    )

    assert result.planned_files == 1
    assert result.verified_files == 1


def test_verify_stage_size_mode_fails_on_missing_file(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_file = source_root / "a.txt"
    source_file.write_bytes(b"x")

    stage_root = tmp_path / "stage_root"
    stage_root.mkdir()
    # intentionally do NOT create stage_root / "a.txt"

    candidates = [DummyCandidate(source_file, Path("a.txt"))]
    with pytest.raises(RestoreVerificationError):
        verify_restore_stage(
            candidates=candidates,
            stage_root=stage_root,
            verification_mode="size",
            dry_run=False,
            journal=None,
        )


def test_verify_stage_size_mode_fails_on_size_mismatch(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_file = source_root / "a.txt"
    source_file.write_bytes(b"xx")

    stage_root = tmp_path / "stage_root"
    stage_root.mkdir()
    (stage_root / "a.txt").write_bytes(b"x")

    candidates = [DummyCandidate(source_file, Path("a.txt"))]
    with pytest.raises(RestoreVerificationError):
        verify_restore_stage(
            candidates=candidates,
            stage_root=stage_root,
            verification_mode="size",
            dry_run=False,
            journal=None,
        )
