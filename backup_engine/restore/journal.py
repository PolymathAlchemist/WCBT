from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol


class Clock(Protocol):
    """
    Injectable time source used to make restore execution deterministic.

    This protocol defines the minimal interface required for time access.
    Implementations may return real time (production) or fixed / simulated
    time (tests, replay, or debugging).
    """

    def now(self) -> datetime:
        """
        Return the current time.

        Returns
        -------
        datetime
            Current timezone-aware datetime.
        """


@dataclass(frozen=True)
class JournalEvent:
    """
    A single append-only journal record.

    Attributes
    ----------
    timestamp:
        Event time (timezone-aware).
    event:
        Stable event identifier (e.g., 'promotion_planned', 'promotion_started').
    data:
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
    Each call appends exactly one JSON object as a single line (JSONL).
    Writes are append-only and do not modify existing records.
    """

    def __init__(self, journal_path: Path, *, clock: Clock) -> None:
        self._journal_path = journal_path
        self._clock = clock
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        """
        Return the on-disk path to the journal file.

        Returns
        -------
        pathlib.Path
            Path to the JSONL journal file.
        """
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
