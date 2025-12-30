from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from .errors import RestoreError
from .journal import RestoreExecutionJournal


class RestoreVerificationError(RestoreError):
    """Raised when staged restore verification fails."""


@dataclass(frozen=True)
class VerificationResult:
    """
    Result of verifying staged restore content.

    Attributes
    ----------
    verified_files : int
        Number of files verified successfully.
    planned_files : int
        Total number of files considered for verification.
    verification_mode : str
        Verification mode applied ('none' or 'size').
    """

    verified_files: int
    planned_files: int
    verification_mode: str


def _candidate_to_dict(candidate: Any) -> Mapping[str, Any]:
    if hasattr(candidate, "to_dict") and callable(candidate.to_dict):
        result = candidate.to_dict()
        if isinstance(result, Mapping):
            return result
    if isinstance(candidate, Mapping):
        return candidate
    raise RestoreVerificationError(
        f"Restore candidate is not dict-like and has no to_dict(): {type(candidate)!r}"
    )


def _coerce_path(value: Any) -> Optional[Path]:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        return Path(value)
    return None


def _extract_relative_destination_path(candidate_dict: Mapping[str, Any]) -> Path:
    dest_keys = ("relative_path", "destination_relative_path", "relpath", "dest_relpath")
    rel_dest: Optional[Path] = None
    for key in dest_keys:
        rel_dest = _coerce_path(candidate_dict.get(key))
        if rel_dest is not None:
            break

    if rel_dest is None:
        raise RestoreVerificationError(
            "Restore candidate missing relative destination path. "
            f"Keys present: {sorted(candidate_dict.keys())}"
        )

    if rel_dest.is_absolute():
        raise RestoreVerificationError(f"Candidate destination must be relative, got: {rel_dest}")

    return rel_dest


def _extract_expected_size(candidate_dict: Mapping[str, Any]) -> Optional[int]:
    size_keys = ("size_bytes", "expected_size_bytes", "expected_size", "size")
    for key in size_keys:
        raw = candidate_dict.get(key)
        if raw is None:
            continue
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.isdigit():
            return int(raw)
    return None


def _extract_source_path(candidate_dict: Mapping[str, Any]) -> Path:
    source_keys = ("source_path", "source", "src", "archive_path")
    source_path = None
    for key in source_keys:
        source_path = _coerce_path(candidate_dict.get(key))
        if source_path is not None:
            break
    if source_path is None:
        raise RestoreVerificationError(
            f"Restore candidate missing source path. Keys present: {sorted(candidate_dict.keys())}"
        )
    return source_path


def verify_restore_stage(
    *,
    candidates: list[Any],
    stage_root: Path,
    verification_mode: str,
    dry_run: bool,
    journal: RestoreExecutionJournal | None = None,
) -> VerificationResult:
    """
    Verify staged restore files before promotion.

    Parameters
    ----------
    candidates:
        Restore candidates produced by materialization. Must be dict-like or support `to_dict()`.
        Must include a relative destination path key.
    stage_root:
        Root directory containing staged restore content.
    verification_mode:
        Verification mode string. Supported: 'none', 'size'.
    journal:
        Optional execution journal.

    Returns
    -------
    VerificationResult
        Summary of verification.

    Raises
    ------
    RestoreVerificationError
        If verification fails (missing file, size mismatch, unsupported mode).
    """
    mode = verification_mode.strip().lower()
    if mode not in {"none", "size"}:
        raise RestoreVerificationError(f"Unsupported verification_mode: {verification_mode!r}")

    planned_files = len(candidates)
    verified_files = 0

    if journal is not None:
        journal.append(
            "verify_stage_planned",
            {
                "stage_root": str(stage_root),
                "candidates_count": planned_files,
                "verification_mode": mode,
            },
        )

    if dry_run:
        if journal is not None:
            journal.append(
                "verify_stage_dry_run",
                {"result": "skipped", "verification_mode": mode, "planned_files": planned_files},
            )
        return VerificationResult(
            verified_files=planned_files,
            planned_files=planned_files,
            verification_mode=mode,
        )

    if mode == "none":
        if journal is not None:
            journal.append("verify_stage_skipped", {"reason": "verification_mode_none"})
        return VerificationResult(
            verified_files=planned_files,
            planned_files=planned_files,
            verification_mode=mode,
        )

    if not stage_root.exists() or not stage_root.is_dir():
        raise RestoreVerificationError(f"Stage root missing or not a directory: {stage_root}")

    if journal is not None:
        journal.append("verify_stage_started", {"stage_root": str(stage_root), "mode": mode})

    for index, candidate in enumerate(candidates):
        candidate_dict = _candidate_to_dict(candidate)
        rel_dest = _extract_relative_destination_path(candidate_dict)
        staged_path = stage_root / rel_dest

        if not staged_path.exists() or not staged_path.is_file():
            raise RestoreVerificationError(f"Missing staged file: {staged_path}")

        source_path = _extract_source_path(candidate_dict)
        if not source_path.exists() or not source_path.is_file():
            raise RestoreVerificationError(f"Source file missing or not a file: {source_path}")

        expected_size = source_path.stat().st_size
        actual_size = staged_path.stat().st_size
        if actual_size != expected_size:
            raise RestoreVerificationError(
                f"Size mismatch for {staged_path}: expected {expected_size}, got {actual_size}"
            )

        actual_size = staged_path.stat().st_size
        if actual_size != expected_size:
            raise RestoreVerificationError(
                f"Size mismatch for {staged_path}: expected {expected_size}, got {actual_size}"
            )

        verified_files += 1

        if journal is not None and (
            index == 0 or (index + 1) % 500 == 0 or (index + 1) == planned_files
        ):
            journal.append(
                "verify_stage_progress",
                {"verified_files": verified_files, "planned_files": planned_files},
            )

    if journal is not None:
        journal.append(
            "verify_stage_completed",
            {"verified_files": verified_files, "planned_files": planned_files, "mode": mode},
        )

    return VerificationResult(
        verified_files=verified_files,
        planned_files=planned_files,
        verification_mode=mode,
    )
