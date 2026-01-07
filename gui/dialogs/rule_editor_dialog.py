"""
Rule Editor dialog (UI only).

Purpose:
- Provide a dedicated Add/Edit dialog for glob-based rules.
- Include inline, collapsible syntax help (UX docstring).
- Preview area (not implemented yet; no scanning)

No WCBT engine wiring. No validation beyond basic non-empty pattern.
"""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


@dataclass(frozen=True, slots=True)
class RuleEditorResult:
    pattern: str


class RuleEditorDialog(QDialog):
    """
    UI-only dialog for adding/editing a rule pattern.

    - Root-relative, / separators, glob syntax.
    - "Future backups only" messaging.
    - Collapsible help area.
    - Preview region (static text for now).
    """

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        title: str,
        initial_pattern: str = "",
        mode_label: str = "Rule",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(640, 420)

        self._result: RuleEditorResult | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # Banner
        banner = QLabel(
            "Edits affect future backups only. Existing archives and history are not modified."
        )
        banner.setWordWrap(True)
        banner.setStyleSheet(
            "background:#2b2b2b; border:1px solid #555; color:#ddd; padding:6px; font-size:12px;"
        )
        root.addWidget(banner)

        # Pattern label + editor
        row = QWidget()
        row_layout = QVBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(6)

        lbl = QLabel(f"{mode_label} pattern")
        row_layout.addWidget(lbl)

        self.pattern_edit = QLineEdit()
        self.pattern_edit.setFont(_mono())
        self.pattern_edit.setPlaceholderText("Example: logs/**  or  *.json")
        self.pattern_edit.setText(initial_pattern)
        self.pattern_edit.setToolTip(
            "Root-relative glob pattern. Use / separators. * and ** are supported."
        )
        row_layout.addWidget(self.pattern_edit)

        root.addWidget(row)

        # Collapsible help (syntax docstring)
        help_box = self._build_collapsible_help()
        root.addWidget(help_box)

        # Preview area
        preview = self._build_preview()
        root.addWidget(preview, 1)

        # Buttons
        self.buttons = QDialogButtonBox(QDialogButtonBox.Cancel)
        self.btn_save = self.buttons.addButton("Save rule", QDialogButtonBox.AcceptRole)
        self.btn_save.setEnabled(False)

        self.buttons.rejected.connect(self.reject)
        self.btn_save.clicked.connect(self._on_save)

        root.addWidget(self.buttons)

        # State wiring
        self.pattern_edit.textChanged.connect(self._sync_state)
        self._sync_state()

    def result_value(self) -> RuleEditorResult | None:
        return self._result

    # ---------------- UI sections ----------------

    def _build_collapsible_help(self) -> QWidget:
        box = QWidget()
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)

        self.help_toggle = QToolButton()
        self.help_toggle.setText("Rule syntax help")
        self.help_toggle.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.help_toggle.setArrowType(Qt.RightArrow)
        self.help_toggle.setCheckable(True)
        self.help_toggle.setChecked(False)
        self.help_toggle.toggled.connect(self._toggle_help)

        header_layout.addWidget(self.help_toggle)
        header_layout.addStretch(1)
        layout.addWidget(header)

        self.help_panel = QFrame()
        self.help_panel.setFrameShape(QFrame.StyledPanel)
        self.help_panel.setVisible(False)

        hp = QVBoxLayout(self.help_panel)
        hp.setContentsMargins(10, 10, 10, 10)

        help_text = QPlainTextEdit()
        help_text.setReadOnly(True)
        help_text.setFont(_mono())
        help_text.setPlainText(
            "\n".join(
                [
                    "Basics",
                    "  • Patterns are relative to the job root",
                    "  • Use / as a path separator",
                    "  • *  matches any characters except /",
                    "  • ** matches across directories",
                    "",
                    "Examples",
                    "  logs/**               → exclude all logs",
                    "  *.json                → match JSON files at any depth",
                    "  config/**/secrets*    → match nested secret files",
                ]
            )
        )

        hp.addWidget(help_text)
        layout.addWidget(self.help_panel)

        return box

    def _build_preview(self) -> QWidget:
        box = QWidget()
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        hint = QLabel(
            "Planned: syntax validation, estimated match counts, and sample paths (no changes to disk)."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(hint)

        hint = QLabel(
            "Planned: syntax validation, estimated match counts, and sample paths (no changes to disk)."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(hint)

        self.preview_text = QPlainTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setFont(_mono())
        self.preview_text.setPlainText(
            "\n".join(
                [
                    "Preview (not implemented yet)",
                    "",
                    "This build does not scan the filesystem or estimate matches.",
                ]
            )
        )
        layout.addWidget(self.preview_text, 1)

        return box

    # ---------------- Behavior ----------------

    def _toggle_help(self, is_open: bool) -> None:
        self.help_panel.setVisible(is_open)
        self.help_toggle.setArrowType(Qt.DownArrow if is_open else Qt.RightArrow)

    def _sync_state(self) -> None:
        # UI-only guardrails: non-empty pattern required to save.
        text = self.pattern_edit.text().strip()
        self.btn_save.setEnabled(bool(text))

        # Preview update (honest placeholder; no scanning yet).
        if text:
            self.preview_text.setPlainText(
                "\n".join(
                    [
                        "Preview (not implemented yet)",
                        f"  pattern: {text}",
                        "",
                        "This build does not scan the filesystem or estimate matches.",
                        "Save the rule, then run a backup to see real results in produced artifacts.",
                    ]
                )
            )
        else:
            self.preview_text.setPlainText(
                "\n".join(
                    [
                        "Preview (not implemented yet)",
                        "",
                        "Enter a pattern to see it echoed here.",
                    ]
                )
            )

    def _on_save(self) -> None:
        pattern = self.pattern_edit.text().strip()
        if not pattern:
            return
        self._result = RuleEditorResult(pattern=pattern)
        self.accept()
