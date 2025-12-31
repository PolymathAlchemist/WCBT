from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class RestoreCopyOutcome(str, Enum):
    """Outcome of attempting to stage one restore candidate."""

    COPIED = "copied"
    SKIPPED_DRY_RUN = "skipped_dry_run"
    FAILED = "failed"


@dataclass(frozen=True)
class RestoreCopyResult:
    """
    Result of staging a single restore candidate.

    Attributes
    ----------
    candidate_index:
        Index of the candidate in the staged execution order.
    source_path:
        Absolute path to the source file.
    relative_path:
        Relative destination path under stage_root.
    stage_path:
        Absolute path to the staged destination (stage_root / relative_path).
    outcome:
        Outcome of the copy attempt.
    message:
        Optional human-readable detail (e.g., error message on failure).
    """

    candidate_index: int
    source_path: str
    relative_path: str
    stage_path: str
    outcome: RestoreCopyOutcome
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["outcome"] = self.outcome.value
        return payload


@dataclass(frozen=True)
class RestoreCopySummary:
    """
    Summary of staging copy execution.

    Attributes
    ----------
    status:
        "success" if all candidates were staged, otherwise "failed".
    planned_files:
        Total candidates planned.
    staged_files:
        Total candidates successfully staged (copied) in this run.
    failed_files:
        Total candidates that failed.
    """

    status: str
    planned_files: int
    staged_files: int
    failed_files: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """Append one JSON object as a single line to a JSONL file."""
    line = json.dumps(payload, ensure_ascii=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
        handle.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON with stable UTF-8 encoding."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")
