from pathlib import Path

from backup_engine.restore.execute import promote_stage_to_destination


def test_promote_stage_to_destination_into_missing_target(tmp_path: Path) -> None:
    stage_root = tmp_path / "stage_root"
    destination_root = tmp_path / "destination"

    stage_root.mkdir()
    (stage_root / "a.txt").write_text("hello", encoding="utf-8")

    outcome = promote_stage_to_destination(
        stage_root=stage_root,
        destination_root=destination_root,
        run_id="r1",
        dry_run=False,
        journal=None,
    )

    assert outcome.promoted is True
    assert destination_root.exists()
    assert not stage_root.exists()
    assert (destination_root / "a.txt").read_text(encoding="utf-8") == "hello"


def test_promote_stage_to_destination_preserves_existing_target(tmp_path: Path) -> None:
    stage_root = tmp_path / "stage_root"
    destination_root = tmp_path / "destination"

    stage_root.mkdir()
    destination_root.mkdir()

    (stage_root / "new.txt").write_text("new", encoding="utf-8")
    (destination_root / "old.txt").write_text("old", encoding="utf-8")

    outcome = promote_stage_to_destination(
        stage_root=stage_root,
        destination_root=destination_root,
        run_id="r2",
        dry_run=False,
        journal=None,
    )

    assert outcome.promoted is True
    assert (destination_root / "new.txt").exists()
    assert outcome.previous_root is not None
    assert (outcome.previous_root / "old.txt").exists()
