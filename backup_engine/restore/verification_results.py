from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class RestoreVerifyOutcome(str, Enum):
    """
    Outcome of verifying a single staged restore candidate.

    Values
    ------
    verified:
        Candidate verified successfully.
    skipped:
        Verification was skipped for this candidate.
    failed:
        Candidate verification failed.
    """

    VERIFIED = "verified"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class RestoreVerifyResult:
    """
    Result of verifying one staged restore candidate.

    Attributes
    ----------
    candidate_index:
        Index of the candidate in the verification order.
    relative_path:
        Relative destination path under stage_root.
    staged_path:
        Full path to the staged file.
    outcome:
        Verification outcome for this candidate.
    message:
        Optional detail for failures or skipped reasons.
    """

    candidate_index: int
    relative_path: str
    staged_path: str
    outcome: RestoreVerifyOutcome
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["outcome"] = self.outcome.value
        return payload


@dataclass(frozen=True)
class RestoreVerifySummary:
    """
    Summary of restore stage verification.

    Attributes
    ----------
    status:
        Overall status for the verification step: "success", "failed", or "skipped".
    verification_mode:
        Verification mode applied ("none" or "size").
    planned_files:
        Total candidates considered.
    verified_files:
        Number of candidates verified successfully (or treated as verified in "none" mode).
    failed_files:
        Number of failed candidates.
    """

    status: str
    verification_mode: str
    planned_files: int
    verified_files: int
    failed_files: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """
    Append one JSON object as a single line to a JSONL file.

    Parameters
    ----------
    path:
        JSONL file path.
    payload:
        JSON-serializable mapping.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    Write JSON with stable UTF-8 encoding.

    Parameters
    ----------
    path:
        JSON file path.
    payload:
        JSON-serializable mapping.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
