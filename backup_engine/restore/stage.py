from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from .errors import RestoreError
from .execution_results import (
    RestoreCopyOutcome,
    RestoreCopyResult,
    RestoreCopySummary,
    append_jsonl,
    write_json,
)
from .journal import RestoreExecutionJournal


class RestoreStageError(RestoreError):
    """Raised when stage building fails."""


@dataclass(frozen=True)
class StageBuildResult:
    """
    Result of building a restore stage directory.

    Attributes
    ----------
    staged_files : int
        Number of files successfully staged (copied or already present and verified).
    planned_files : int
        Number of files that would be staged in dry-run mode.
    stage_root : Path
        Root directory containing staged restore content.
    """

    staged_files: int
    planned_files: int
    stage_root: Path


def _coerce_path(value: Any) -> Optional[Path]:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        return Path(value)
    return None


def _extract_candidate_paths(candidate_dict: Mapping[str, Any]) -> tuple[Path, Path]:
    """
    Extract (source_path, relative_destination_path) from a candidate mapping.

    This intentionally tolerates a few key variants to avoid tight coupling.

    Raises
    ------
    RestoreStageError
        If required fields cannot be found or destination is not relative.
    """
    source_keys = ("source_path", "source", "src", "archive_path")
    dest_keys = ("relative_path", "destination_relative_path", "relpath", "dest_relpath")

    source_path: Optional[Path] = None
    for key in source_keys:
        source_path = _coerce_path(candidate_dict.get(key))
        if source_path is not None:
            break

    rel_dest: Optional[Path] = None
    for key in dest_keys:
        rel_dest = _coerce_path(candidate_dict.get(key))
        if rel_dest is not None:
            break

    if source_path is None or rel_dest is None:
        raise RestoreStageError(
            "Restore candidate is missing required path fields. "
            f"Keys present: {sorted(candidate_dict.keys())}"
        )

    if rel_dest.is_absolute():
        raise RestoreStageError(
            f"Restore candidate destination path must be relative, got: {rel_dest}"
        )

    return source_path, rel_dest


def _copy_file_atomic(source_path: Path, destination_path: Path) -> None:
    """
    Copy a file to destination atomically by writing to a temp file then renaming.

    Notes
    -----
    - This is restart-friendly: if the temp file exists it is replaced.
    - Rename is atomic within the same directory.
    """
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination_path.with_name(destination_path.name + ".wcbt_tmp")

    if temp_path.exists():
        temp_path.unlink()

    with source_path.open("rb") as src, temp_path.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())

    temp_path.rename(destination_path)


def _candidate_to_dict(candidate: Any) -> Mapping[str, Any]:
    if hasattr(candidate, "to_dict") and callable(candidate.to_dict):
        result = candidate.to_dict()
        if isinstance(result, Mapping):
            return result
    if isinstance(candidate, Mapping):
        return candidate
    raise RestoreStageError(
        f"Restore candidate is not dict-like and has no to_dict(): {type(candidate)!r}"
    )


def build_restore_stage(
    *,
    candidates: list[Any],
    stage_root: Path,
    dry_run: bool,
    journal: RestoreExecutionJournal | None = None,
    artifacts_root: Path | None = None,
) -> StageBuildResult:
    """
    Build a staged restore tree under `stage_root`.

    Parameters
    ----------
    candidates:
        Restore candidates produced by materialization. Each candidate must be dict-like
        or provide a `to_dict()` returning a mapping that includes a source path and
        a relative destination path.
    stage_root:
        Root directory to receive staged content.
    dry_run:
        If True, do not copy any files.
    journal:
        Optional execution journal.
    artifacts_root:
        Optional directory to write stage copy execution artifacts. If provided, this
        function writes:
        - stage_copy_results.jsonl
        - stage_copy_summary.json

    Returns
    -------
    StageBuildResult
        Summary of staging work performed.

    Raises
    ------
    RestoreStageError
        If staging fails due to missing fields, missing source files, or I/O errors.
    """
    planned_files = len(candidates)
    staged_files = 0
    results_path: Path | None = None
    summary_path: Path | None = None
    if artifacts_root is not None:
        results_path = artifacts_root / "stage_copy_results.jsonl"
        summary_path = artifacts_root / "stage_copy_summary.json"

    if journal is not None:
        journal.append(
            "stage_build_planned",
            {"stage_root": str(stage_root), "candidates_count": planned_files, "dry_run": dry_run},
        )

    if dry_run:
        if results_path is not None:
            for index, candidate in enumerate(candidates):
                candidate_dict = _candidate_to_dict(candidate)
                source_path, rel_dest = _extract_candidate_paths(candidate_dict)
                destination_path = stage_root / rel_dest

                result = RestoreCopyResult(
                    candidate_index=index,
                    source_path=str(source_path),
                    relative_path=str(rel_dest),
                    stage_path=str(destination_path),
                    outcome=RestoreCopyOutcome.SKIPPED_DRY_RUN,
                    message="dry_run=True",
                )
                append_jsonl(results_path, result.to_dict())

            summary = RestoreCopySummary(
                status="success",
                planned_files=planned_files,
                staged_files=0,
                failed_files=0,
            )
            assert summary_path is not None
            write_json(summary_path, summary.to_dict())

        if journal is not None:
            journal.append("stage_build_dry_run", {"result": "no_changes"})

        return StageBuildResult(staged_files=0, planned_files=planned_files, stage_root=stage_root)

    stage_root.mkdir(parents=True, exist_ok=True)

    if journal is not None:
        journal.append("stage_build_started", {"stage_root": str(stage_root)})

    for index, candidate in enumerate(candidates):
        candidate_dict = _candidate_to_dict(candidate)
        source_path, rel_dest = _extract_candidate_paths(candidate_dict)

        if not source_path.exists() or not source_path.is_file():
            raise RestoreStageError(f"Source file missing or not a file: {source_path}")

        destination_path = stage_root / rel_dest

        try:
            _copy_file_atomic(source_path, destination_path)
        except OSError as exc:
            if results_path is not None:
                failed = RestoreCopyResult(
                    candidate_index=index,
                    source_path=str(source_path),
                    relative_path=str(rel_dest),
                    stage_path=str(destination_path),
                    outcome=RestoreCopyOutcome.FAILED,
                    message=str(exc),
                )
                append_jsonl(results_path, failed.to_dict())

                summary = RestoreCopySummary(
                    status="failed",
                    planned_files=planned_files,
                    staged_files=staged_files,
                    failed_files=1,
                )
                assert summary_path is not None
                write_json(summary_path, summary.to_dict())

            raise RestoreStageError(
                f"Failed to stage file {source_path} -> {destination_path}"
            ) from exc

        staged_files += 1

        if results_path is not None:
            ok = RestoreCopyResult(
                candidate_index=index,
                source_path=str(source_path),
                relative_path=str(rel_dest),
                stage_path=str(destination_path),
                outcome=RestoreCopyOutcome.COPIED,
            )
            append_jsonl(results_path, ok.to_dict())

        if journal is not None and (
            index == 0 or (index + 1) % 250 == 0 or (index + 1) == planned_files
        ):
            journal.append(
                "stage_build_progress",
                {"staged_files": staged_files, "planned_files": planned_files},
            )

    if journal is not None:
        journal.append(
            "stage_build_completed",
            {"staged_files": staged_files, "planned_files": planned_files},
        )

    if summary_path is not None:
        summary = RestoreCopySummary(
            status="success",
            planned_files=planned_files,
            staged_files=staged_files,
            failed_files=0,
        )
        assert summary_path is not None
        write_json(summary_path, summary.to_dict())

    return StageBuildResult(
        staged_files=staged_files,
        planned_files=planned_files,
        stage_root=stage_root,
    )
