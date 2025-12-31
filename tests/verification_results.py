from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class RestoreVerifyOutcome(str, Enum):
    """Outcome of verifying a single staged file."""

    VERIFIED = "verified"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class RestoreVerifyResult:
    """
    Result of verifying one restore candidate.
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
    """

    status: str
    verification_mode: str
    planned_files: int
    verified_files: int
    failed_files: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
