from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol


class Clock(Protocol):
    """Injectable clock for deterministic behavior."""

    def now(self) -> datetime:
        """Return the current time as a timezone-aware datetime."""


@dataclass(frozen=True)
class JournalEvent:
    """
    A single append-only journal record.

    Parameters
    ----------
    timestamp : datetime
        Event time (timezone-aware).
    event : str
        Stable event identifier (e.g., 'promotion_planned', 'promotion_started').
    data : Mapping[str, Any]
        Structured event payload. Must be JSON-serializable.
    """

    timestamp: datetime
    event: str
    data: Mapping[str, Any]


class RestoreExecutionJournal:
    """
    Append-only JSONL journal for restore execution.

    Notes
    -----
    - Each call to `append()` writes one JSON object per line (JSONL).
    - The journal is an inspectable artifact intended for debugging and audit.
    """

    def __init__(self, journal_path: Path, *, clock: Clock) -> None:
        self._journal_path = journal_path
        self._clock = clock
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        """Return the on-disk path to the journal file."""
        return self._journal_path

    def append(self, event: str, data: Mapping[str, Any]) -> None:
        """
        Append a new event record.

        Parameters
        ----------
        event : str
            Stable event identifier.
        data : Mapping[str, Any]
            JSON-serializable event payload.

        Raises
        ------
        OSError
            If the journal cannot be written.
        TypeError
            If `data` contains non-JSON-serializable values.
        """
        record = JournalEvent(timestamp=self._clock.now(), event=event, data=data)
        line = json.dumps(
            {
                "ts": record.timestamp.isoformat(),
                "event": record.event,
                "data": record.data,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._journal_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line + "\n")
            handle.flush()
