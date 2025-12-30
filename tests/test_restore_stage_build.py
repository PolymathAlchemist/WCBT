from pathlib import Path

from backup_engine.restore.stage import build_restore_stage


class DummyCandidate:
    def __init__(self, source_path: Path, relative_path: Path) -> None:
        self._source_path = source_path
        self._relative_path = relative_path

    def to_dict(self) -> dict[str, str]:
        return {"source_path": str(self._source_path), "relative_path": str(self._relative_path)}


def test_stage_build_copies_files(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_text("hello", encoding="utf-8")

    stage_root = tmp_path / "stage_root"
    candidates = [DummyCandidate(source, Path("nested") / "dest.txt")]

    result = build_restore_stage(
        candidates=candidates, stage_root=stage_root, dry_run=False, journal=None
    )

    assert result.staged_files == 1
    assert (stage_root / "nested" / "dest.txt").read_text(encoding="utf-8") == "hello"


def test_stage_build_dry_run_creates_no_files(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_text("hello", encoding="utf-8")

    stage_root = tmp_path / "stage_root"
    candidates = [DummyCandidate(source, Path("dest.txt"))]

    result = build_restore_stage(
        candidates=candidates, stage_root=stage_root, dry_run=True, journal=None
    )

    assert result.planned_files == 1
    assert result.staged_files == 0
    assert not stage_root.exists()
