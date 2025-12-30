import json
from datetime import datetime, timezone
from pathlib import Path

from backup_engine.restore.journal import RestoreExecutionJournal


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


def test_journal_appends_jsonl_records(tmp_path: Path) -> None:
    journal_path = tmp_path / "journal.jsonl"
    clock = FixedClock(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc))

    journal = RestoreExecutionJournal(journal_path, clock=clock)

    journal.append("event_one", {"a": 1})
    journal.append("event_two", {"b": "x"})

    lines = journal_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    first = json.loads(lines[0])
    second = json.loads(lines[1])

    assert first["ts"] == "2024-01-01T12:00:00+00:00"
    assert first["event"] == "event_one"
    assert first["data"] == {"a": 1}

    assert second["event"] == "event_two"
    assert second["data"] == {"b": "x"}
