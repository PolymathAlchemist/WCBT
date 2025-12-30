from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from backup_engine.restore.execute import (
    PromotionError,
    execute_promotion,
    plan_promotion,
)


def test_promote_into_missing_target(tmp_path: Path) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    (stage / "file.txt").write_text("data")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r1")
    outcome = execute_promotion(plan=plan, dry_run=False)

    assert outcome.promoted is True
    assert target.exists()
    assert not stage.exists()
    assert (target / "file.txt").read_text() == "data"


def test_promote_over_existing_target(tmp_path: Path) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    target.mkdir()

    (stage / "new.txt").write_text("new")
    (target / "old.txt").write_text("old")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r2")
    outcome = execute_promotion(plan=plan, dry_run=False)

    assert outcome.promoted is True
    assert target.exists()
    assert (target / "new.txt").exists()
    assert outcome.previous_root is not None
    assert (outcome.previous_root / "old.txt").exists()


def test_failure_on_first_rename_leaves_state_intact(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:

    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    target.mkdir()

    def fail_once(self: Path, other: Path) -> None:
        raise OSError("boom")

    monkeypatch.setattr(Path, "rename", fail_once)

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r3")

    with pytest.raises(PromotionError):
        execute_promotion(plan=plan, dry_run=False)

    assert stage.exists()
    assert target.exists()
