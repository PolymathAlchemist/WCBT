"""Qt adapter for engine ProfileStore.

The engine owns persistence. The GUI talks to this adapter via signals/slots to
avoid blocking the UI thread and to avoid exposing SQLite or engine internals.

Threading model
--------------
- A single worker QObject lives on a dedicated QThread.
- The worker owns the SqliteProfileStore and its sqlite3 connection usage.
- The GUI communicates with the worker via queued Qt signals.

This design is deliberately small and reusable for future long-running engine
operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QObject, Qt, QThread, Signal, Slot

from backup_engine.profile_store.api import RuleSet
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import open_profile_store


@dataclass(frozen=True, slots=True)
class GuiRuleSet:
    """GUI-friendly representation of a job rule set."""

    include: tuple[str, ...]
    exclude: tuple[str, ...]


class ProfileStoreWorker(QObject):
    """Worker that owns the engine ProfileStore and runs in a background thread."""

    rules_loaded = Signal(str, object)  # job_id, GuiRuleSet
    rules_saved = Signal(str)  # job_id
    unknown_job = Signal(str)  # job_id
    error = Signal(str, str)  # job_id, message

    jobs_loaded = Signal(object)  # list[JobSummary]
    job_created = Signal(str)  # job_id
    job_renamed = Signal(str)  # job_id
    job_deleted = Signal(str)  # job_id

    def __init__(self, profile_name: str, data_root: Path | None) -> None:
        super().__init__()
        self._store = open_profile_store(profile_name=profile_name, data_root=data_root)

    @Slot()
    def list_jobs(self) -> None:
        """List known jobs and emit results."""
        try:
            jobs = list(self._store.list_jobs())
        except Exception as e:
            self.error.emit("", str(e))
            return
        self.jobs_loaded.emit(jobs)

    @Slot(str)
    def create_job(self, name: str) -> None:
        """Create a job and emit the new job_id."""
        try:
            job_id = self._store.create_job(name)
        except Exception as e:
            self.error.emit("", str(e))
            return
        self.job_created.emit(job_id)

    @Slot(str, str)
    def rename_job(self, job_id: str, new_name: str) -> None:
        """Rename job_id and emit completion."""
        try:
            self._store.rename_job(job_id, new_name)
        except UnknownJobError:
            self.unknown_job.emit(job_id)
            return
        except Exception as e:
            self.error.emit(job_id, str(e))
            return
        self.job_renamed.emit(job_id)

    @Slot(str)
    def delete_job(self, job_id: str) -> None:
        """Delete job_id and emit completion."""
        try:
            self._store.delete_job(job_id)
        except UnknownJobError:
            self.unknown_job.emit(job_id)
            return
        except Exception as e:
            self.error.emit(job_id, str(e))
            return
        self.job_deleted.emit(job_id)

    @Slot(str)
    def load_rules(self, job_id: str) -> None:
        """Load rules for job_id and emit results."""
        try:
            rules = self._store.load_rules(job_id)
        except UnknownJobError:
            self.unknown_job.emit(job_id)
            return
        except Exception as e:
            self.error.emit(job_id, str(e))
            return

        self.rules_loaded.emit(job_id, GuiRuleSet(include=rules.include, exclude=rules.exclude))

    @Slot(str, str, object)
    def save_rules(self, job_id: str, name: str, rules: object) -> None:
        """Save rules for job_id and emit completion."""
        try:
            gui_rules = rules
            assert isinstance(gui_rules, GuiRuleSet)
            self._store.save_rules(
                job_id=job_id,
                name=name,
                rules=RuleSet(include=gui_rules.include, exclude=gui_rules.exclude),
            )
        except Exception as e:
            self.error.emit(job_id, str(e))
            return

        self.rules_saved.emit(job_id)


class ProfileStoreAdapter(QObject):
    """Qt adapter that marshals ProfileStore calls onto a worker thread."""

    # Requests (GUI emits these; wired as queued connections to worker slots)
    request_load_rules = Signal(str)
    request_save_rules = Signal(str, str, object)
    request_list_jobs = Signal()
    request_create_job = Signal(str)
    request_rename_job = Signal(str, str)
    request_delete_job = Signal(str)

    # Results (worker emits; adapter forwards)
    rules_loaded = Signal(str, object)  # job_id, GuiRuleSet
    rules_saved = Signal(str)  # job_id
    unknown_job = Signal(str)  # job_id
    error = Signal(str, str)  # job_id, message
    jobs_loaded = Signal(object)  # list[JobSummary]
    job_created = Signal(str)  # job_id
    job_renamed = Signal(str)  # job_id
    job_deleted = Signal(str)  # job_id

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__()

        self._thread = QThread()
        self._worker = ProfileStoreWorker(profile_name=profile_name, data_root=data_root)
        self._worker.moveToThread(self._thread)

        # Queue requests onto worker thread.
        self.request_load_rules.connect(
            self._worker.load_rules, type=Qt.ConnectionType.QueuedConnection
        )
        self.request_save_rules.connect(
            self._worker.save_rules, type=Qt.ConnectionType.QueuedConnection
        )
        self.request_list_jobs.connect(
            self._worker.list_jobs, type=Qt.ConnectionType.QueuedConnection
        )
        self.request_create_job.connect(
            self._worker.create_job, type=Qt.ConnectionType.QueuedConnection
        )
        self.request_rename_job.connect(
            self._worker.rename_job, type=Qt.ConnectionType.QueuedConnection
        )
        self.request_delete_job.connect(
            self._worker.delete_job, type=Qt.ConnectionType.QueuedConnection
        )

        # Forward results to GUI.
        self._worker.rules_loaded.connect(self.rules_loaded)
        self._worker.rules_saved.connect(self.rules_saved)
        self._worker.unknown_job.connect(self.unknown_job)
        self._worker.error.connect(self.error)
        self._worker.jobs_loaded.connect(self.jobs_loaded)
        self._worker.job_created.connect(self.job_created)
        self._worker.job_renamed.connect(self.job_renamed)
        self._worker.job_deleted.connect(self.job_deleted)

        self._thread.start()

    def shutdown(self) -> None:
        """Stop the worker thread cleanly."""
        self._thread.quit()
        self._thread.wait()
