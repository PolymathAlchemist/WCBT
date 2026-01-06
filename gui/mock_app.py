"""
WCBT GUI mock app.

Runs a multi-tab UI with per-tab modules so we can iterate quickly on layout.
No core wiring.
"""

from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication, QHBoxLayout, QLabel, QTabWidget, QVBoxLayout, QWidget

from gui.mock_data import seed_jobs, seed_runs
from gui.tabs.authoring_tab import AuthoringTab
from gui.tabs.mock_restore_tab import RestoreTab
from gui.tabs.mock_run_tab import RunTab


class MockApp(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("WCBT GUI Mock (Tabbed, No Engine Wiring)")
        self.resize(1180, 720)

        jobs = seed_jobs()
        runs = seed_runs()

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(8, 8, 8, 8)

        title = QLabel("WCBT")
        f = title.font()
        f.setPointSize(16)
        f.setBold(True)
        title.setFont(f)

        subtitle = QLabel("GUI mock for UX iteration (no core wiring)")
        subtitle.setStyleSheet("color: #666;")

        header_layout.addWidget(title)
        header_layout.addSpacing(10)
        header_layout.addWidget(subtitle)
        header_layout.addStretch(1)

        root.addWidget(header)

        tabs = QTabWidget()
        tabs.addTab(RunTab(jobs), "Run")
        tabs.addTab(RestoreTab(jobs, runs), "Restore")
        self.authoring_tab = AuthoringTab()
        tabs.addTab(self.authoring_tab, "Authoring")

        root.addWidget(tabs, 1)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """Shutdown background workers before closing the application."""
        try:
            # If we stored the authoring tab instance, shut it down.
            if hasattr(self, "authoring_tab"):
                self.authoring_tab.shutdown()  # type: ignore[attr-defined]
        finally:
            super().closeEvent(event)


def main() -> int:
    app = QApplication(sys.argv)
    w = MockApp()
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
