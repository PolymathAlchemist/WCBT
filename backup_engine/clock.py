"""
Clock abstractions for deterministic behavior.

Notes
-----
Engine code must not access wall-clock time directly. Callers provide a Clock.
This enables deterministic tests and reproducible identifiers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    """A source of time for deterministic behavior."""

    def now(self) -> datetime:
        """
        Return the current time.

        Returns
        -------
        datetime
            A timezone-aware datetime.
        """
        ...


@dataclass(frozen=True, slots=True)
class SystemClock:
    """Clock that returns the current system time in UTC."""

    def now(self) -> datetime:
        """
        Return the current system time.

        Returns
        -------
        datetime
            Current time as a timezone-aware datetime in UTC.
        """
        return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class FixedClock:
    """Clock that always returns a fixed time (useful for tests)."""

    fixed_time: datetime

    def now(self) -> datetime:
        """
        Return the fixed time.

        Returns
        -------
        datetime
            The fixed time value.
        """
        if self.fixed_time.tzinfo is None:
            return self.fixed_time.replace(tzinfo=timezone.utc)
        return self.fixed_time
