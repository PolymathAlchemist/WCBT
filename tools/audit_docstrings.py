"""
Docstring audit utility for WCBT.

This script reports missing docstrings for public, module-scope APIs
(functions/classes) in the codebase.

Rules:
- Only checks module-scope `def` / `class` definitions.
- Ignores private names (starting with `_`).
- Excludes virtualenv and cache directories.
- Excludes tests and already-certified subsystems (backup/restore/profile_lock).
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Finding:
    """A missing-docstring finding for a public module-scope definition."""

    path: Path
    lineno: int
    kind: str
    name: str

    def render(self) -> str:
        """Render a stable, copy/paste-friendly finding line."""
        return f"{self.path.as_posix()}:{self.lineno}: public {self.kind} '{self.name}' missing docstring"


EXCLUDE_DIR_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
}

EXCLUDE_TOP_LEVEL_DIRS = {
    "tests",
}

EXCLUDE_PREFIXES = (
    "backup_engine/backup/",
    "backup_engine/restore/",
    "backup_engine/profile_lock/",
)


def _is_excluded(path: Path, repo_root: Path) -> bool:
    """
    Return True if `path` should be excluded from auditing.

    Parameters
    ----------
    path
        The file path to consider.
    repo_root
        Repository root used to compute a stable relative path.

    Returns
    -------
    bool
        True if the file is excluded.
    """
    rel = path.relative_to(repo_root).as_posix()

    if any(part in EXCLUDE_DIR_PARTS for part in path.parts):
        return True

    top = rel.split("/", 1)[0]
    if top in EXCLUDE_TOP_LEVEL_DIRS:
        return True

    return rel.startswith(EXCLUDE_PREFIXES)


def _audit_module_scope_defs(py_file: Path) -> list[Finding]:
    """
    Audit a single Python file for missing docstrings on public module-scope defs.

    Parameters
    ----------
    py_file
        The Python source file to parse.

    Returns
    -------
    list[Finding]
        Missing-docstring findings for module-scope functions/classes.
    """
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))

    findings: list[Finding] = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            name = node.name
            if not name.startswith("_") and ast.get_docstring(node) is None:
                findings.append(Finding(py_file, node.lineno, "class", name))

        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = node.name
            if not name.startswith("_") and ast.get_docstring(node) is None:
                findings.append(Finding(py_file, node.lineno, "function", name))

    return findings


def main(argv: list[str]) -> int:
    """
    Run the audit and print findings.

    Parameters
    ----------
    argv
        Command-line arguments (unused, present for conventional structure).

    Returns
    -------
    int
        Process exit code: 0 if no findings, 1 if findings exist.
    """
    _ = argv
    repo_root = Path.cwd()

    findings: list[Finding] = []
    for py_file in repo_root.rglob("*.py"):
        if _is_excluded(py_file, repo_root):
            continue
        findings.extend(_audit_module_scope_defs(py_file))

    if not findings:
        print("✓ No missing public docstrings detected")
        return 0

    for finding in sorted(findings, key=lambda f: (f.path.as_posix(), f.lineno, f.name)):
        print(finding.render())
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
