"""
Docstring audit utility for WCBT.

This script reports missing docstrings for public, module-scope APIs
(functions/classes) in the codebase.

By default, it:
- Checks only module-scope `def` / `class` definitions.
- Ignores private names (starting with `_`).
- Excludes virtualenv and cache directories.
- Excludes tests and already-certified subsystems (backup/restore/profile_lock).

Use CLI flags to broaden scope or add module-docstring checks.
"""

from __future__ import annotations

import argparse
import ast
import json
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


@dataclass(frozen=True, slots=True)
class Config:
    """Configuration for docstring auditing."""

    include_tests: bool
    include_certified: bool
    check_module_docstrings: bool


EXCLUDE_DIR_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
}

CERTIFIED_PREFIXES = (
    "backup_engine/backup/",
    "backup_engine/restore/",
    "backup_engine/profile_lock/",
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Parameters
    ----------
    argv
        Raw command-line arguments (excluding program name).

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Audit missing docstrings for public module-scope APIs."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Repository root to audit (default: current working directory).",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include tests/ in the audit (default: excluded).",
    )
    parser.add_argument(
        "--include-certified",
        action="store_true",
        help="Include already-certified subsystems (backup/restore/profile_lock) (default: excluded).",
    )
    parser.add_argument(
        "--check-module-docstrings",
        action="store_true",
        help="Also flag missing module docstrings (file header docstrings) (default: off).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit findings as JSON (default: human-readable lines).",
    )
    return parser.parse_args(argv)


def _is_excluded(py_file: Path, repo_root: Path, config: Config) -> bool:
    """
    Return True if `py_file` should be excluded from auditing.

    Parameters
    ----------
    py_file
        The file path to consider.
    repo_root
        Repository root used to compute a stable relative path.
    config
        Audit configuration controlling inclusion of tests/certified subsystems.

    Returns
    -------
    bool
        True if the file is excluded.
    """
    if any(part in EXCLUDE_DIR_PARTS for part in py_file.parts):
        return True

    rel = py_file.relative_to(repo_root).as_posix()
    top = rel.split("/", 1)[0]

    if (not config.include_tests) and top == "tests":
        return True

    if (not config.include_certified) and rel.startswith(CERTIFIED_PREFIXES):
        return True

    return False


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
            if not node.name.startswith("_") and ast.get_docstring(node) is None:
                findings.append(Finding(py_file, node.lineno, "class", node.name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_") and ast.get_docstring(node) is None:
                findings.append(Finding(py_file, node.lineno, "function", node.name))

    return findings


def _audit_module_docstring(py_file: Path) -> Finding | None:
    """
    Audit a single Python file for a missing module docstring.

    Parameters
    ----------
    py_file
        The Python source file to parse.

    Returns
    -------
    Finding | None
        A finding if the module docstring is missing, otherwise None.
    """
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))

    module_doc = ast.get_docstring(tree)
    if module_doc is None:
        # Convention: point at line 1 for module-docstring findings.
        return Finding(py_file, 1, "module", py_file.name)
    return None


def audit(repo_root: Path, config: Config) -> list[Finding]:
    """
    Audit a repository tree for missing docstrings.

    Parameters
    ----------
    repo_root
        Root directory to scan.
    config
        Audit configuration controlling inclusion and checks.

    Returns
    -------
    list[Finding]
        All missing-docstring findings.
    """
    findings: list[Finding] = []

    for py_file in repo_root.rglob("*.py"):
        if _is_excluded(py_file, repo_root, config):
            continue

        if config.check_module_docstrings:
            module_finding = _audit_module_docstring(py_file)
            if module_finding is not None:
                findings.append(module_finding)

        findings.extend(_audit_module_scope_defs(py_file))

    return sorted(findings, key=lambda f: (f.path.as_posix(), f.lineno, f.name))


def main(argv: list[str]) -> int:
    """
    Run the audit and print findings.

    Parameters
    ----------
    argv
        Command-line arguments excluding the program name.

    Returns
    -------
    int
        Process exit code: 0 if no findings, 1 if findings exist.
    """
    args = _parse_args(argv)
    repo_root: Path = args.root.resolve()

    config = Config(
        include_tests=bool(args.include_tests),
        include_certified=bool(args.include_certified),
        check_module_docstrings=bool(args.check_module_docstrings),
    )

    findings = audit(repo_root, config)

    if not findings:
        print("✓ No missing public docstrings detected")
        return 0

    if bool(args.json):
        payload = [
            {"path": f.path.as_posix(), "lineno": f.lineno, "kind": f.kind, "name": f.name}
            for f in findings
        ]
        print(json.dumps(payload, indent=2))
        return 1

    for finding in findings:
        print(finding.render())
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
