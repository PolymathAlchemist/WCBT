import shutil
from pathlib import Path
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from backup_engine.restore.execute import (
    PromotionError,
    _promote_overwrite_filewise,
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


def test_atomic_overwrite_path_remains_primary_when_rename_succeeds(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    target.mkdir()

    (stage / "new.txt").write_text("new")
    (target / "old.txt").write_text("old")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r2a")

    def fail_if_filewise_used(*, stage_root: Path, target_root: Path) -> None:
        raise AssertionError(
            f"filewise fallback should not be used when rename succeeds: {stage_root} -> {target_root}"
        )

    monkeypatch.setattr(
        "backup_engine.restore.execute._promote_overwrite_filewise",
        fail_if_filewise_used,
    )

    outcome = execute_promotion(plan=plan, dry_run=False)

    assert outcome.promoted is True
    assert outcome.previous_root is not None
    assert (target / "new.txt").read_text() == "new"
    assert (outcome.previous_root / "old.txt").read_text() == "old"


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


def test_locked_live_destination_uses_filewise_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    target.mkdir()
    (stage / "restored.txt").write_text("restored", encoding="utf-8")
    (target / "existing.txt").write_text("existing", encoding="utf-8")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r4")
    assert plan.previous_root is not None

    original_rename = Path.rename

    def fail_target_preserve(self: Path, other: Path) -> None:
        if self == target and other == plan.previous_root:
            exc = PermissionError(
                13, "The process cannot access the file because it is being used by another process"
            )
            setattr(exc, "winerror", 32)
            raise exc
        original_rename(self, other)

    monkeypatch.setattr(Path, "rename", fail_target_preserve)

    outcome = execute_promotion(plan=plan, dry_run=False)

    assert outcome.promoted is True
    assert outcome.previous_root is None
    assert (target / "restored.txt").read_text(encoding="utf-8") == "restored"
    assert (target / "existing.txt").read_text(encoding="utf-8") == "existing"
    assert stage.exists()
    assert target.exists()


def test_winerror_32_on_root_rename_triggers_filewise_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    (stage / "nested").mkdir(parents=True)
    target.mkdir()

    (stage / "root.txt").write_text("from-stage", encoding="utf-8")
    (stage / "nested" / "child.txt").write_text("child", encoding="utf-8")
    (target / "root.txt").write_text("old-root", encoding="utf-8")
    (target / "keep.txt").write_text("keep", encoding="utf-8")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r4a")
    assert plan.previous_root is not None

    original_rename = Path.rename

    def fail_target_preserve(self: Path, other: Path) -> None:
        if self == target and other == plan.previous_root:
            exc = PermissionError(
                13,
                "The process cannot access the file because it is being used by another process",
            )
            setattr(exc, "winerror", 32)
            raise exc
        original_rename(self, other)

    monkeypatch.setattr(Path, "rename", fail_target_preserve)

    outcome = execute_promotion(plan=plan, dry_run=False)

    assert outcome.promoted is True
    assert outcome.previous_root is None
    assert (target / "root.txt").read_text(encoding="utf-8") == "from-stage"
    assert (target / "nested" / "child.txt").read_text(encoding="utf-8") == "child"
    assert (target / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert stage.exists()
    assert not plan.previous_root.exists()


def test_filewise_fallback_reports_exact_blocked_file_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"
    blocked_file = target / "nested" / "blocked.txt"

    (stage / "nested").mkdir(parents=True)
    target.mkdir()

    (stage / "nested" / "blocked.txt").write_text("new", encoding="utf-8")
    (target / "nested").mkdir()
    blocked_file.write_text("old", encoding="utf-8")

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r4b")
    assert plan.previous_root is not None

    original_rename = Path.rename

    def fail_target_preserve(self: Path, other: Path) -> None:
        if self == target and other == plan.previous_root:
            exc = PermissionError(
                13,
                "The process cannot access the file because it is being used by another process",
            )
            setattr(exc, "winerror", 32)
            raise exc
        original_rename(self, other)

    original_filewise = _promote_overwrite_filewise

    def fail_blocked_file(*, stage_root: Path, target_root: Path) -> None:
        original_copy2 = shutil.copy2

        def fail_copy(
            source: Path,
            destination: Path,
            *,
            follow_symlinks: bool = True,
        ) -> Any:
            if Path(destination) == blocked_file:
                exc = PermissionError(
                    13,
                    "The process cannot access the file because it is being used by another process",
                )
                setattr(exc, "winerror", 32)
                raise exc
            return original_copy2(source, destination, follow_symlinks=follow_symlinks)

        monkeypatch.setattr("backup_engine.restore.execute.shutil.copy2", fail_copy)
        original_filewise(stage_root=stage_root, target_root=target_root)

    monkeypatch.setattr(Path, "rename", fail_target_preserve)
    monkeypatch.setattr(
        "backup_engine.restore.execute._promote_overwrite_filewise",
        fail_blocked_file,
    )

    with pytest.raises(
        PromotionError,
        match="another process is using the file",
    ) as exc_info:
        execute_promotion(plan=plan, dry_run=False)

    message = str(exc_info.value)
    assert str(blocked_file) in message
    assert "safeguard backup remains intact" in message


def test_failure_on_second_rename_still_raises_generic_atomic_message(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    stage = tmp_path / "stage"
    target = tmp_path / "target"

    stage.mkdir()
    target.mkdir()

    plan = plan_promotion(stage_root=stage, target_root=target, run_id="r5")
    assert plan.previous_root is not None

    original_rename = Path.rename

    def fail_stage_promote(self: Path, other: Path) -> None:
        if self == stage and other == target:
            raise OSError("boom")
        original_rename(self, other)

    monkeypatch.setattr(Path, "rename", fail_stage_promote)

    with pytest.raises(PromotionError, match="Atomic promotion failed"):
        execute_promotion(plan=plan, dry_run=False)
