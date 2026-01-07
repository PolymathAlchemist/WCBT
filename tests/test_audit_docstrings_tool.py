from __future__ import annotations

from pathlib import Path

from audit_docstrings import Config, audit


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_flags_public_module_scope_missing_docstring(tmp_path: Path) -> None:
    _write(
        tmp_path / "pkg" / "mod.py",
        "def public_api():\n    return 1\n",
    )

    findings = audit(
        tmp_path, Config(include_tests=False, include_certified=True, check_module_docstrings=False)
    )
    assert any(f.kind == "function" and f.name == "public_api" for f in findings)


def test_ignores_private_module_scope(tmp_path: Path) -> None:
    _write(
        tmp_path / "pkg" / "mod.py",
        "def _helper():\n    return 1\n",
    )

    findings = audit(
        tmp_path, Config(include_tests=False, include_certified=True, check_module_docstrings=False)
    )
    assert findings == []


def test_excludes_tests_by_default(tmp_path: Path) -> None:
    _write(
        tmp_path / "tests" / "test_something.py",
        "def public_api():\n    return 1\n",
    )

    findings = audit(
        tmp_path, Config(include_tests=False, include_certified=True, check_module_docstrings=False)
    )
    assert findings == []


def test_includes_tests_when_enabled(tmp_path: Path) -> None:
    _write(
        tmp_path / "tests" / "test_something.py",
        "def public_api():\n    return 1\n",
    )

    findings = audit(
        tmp_path, Config(include_tests=True, include_certified=True, check_module_docstrings=False)
    )
    assert any(f.kind == "function" and f.name == "public_api" for f in findings)


def test_module_docstring_check_is_opt_in(tmp_path: Path) -> None:
    _write(
        tmp_path / "pkg" / "mod.py",
        'def public_api():\n    """Docstring."""\n    return 1\n',
    )

    findings_off = audit(
        tmp_path, Config(include_tests=False, include_certified=True, check_module_docstrings=False)
    )
    assert findings_off == []

    findings_on = audit(
        tmp_path, Config(include_tests=False, include_certified=True, check_module_docstrings=True)
    )
    assert any(f.kind == "module" for f in findings_on)
