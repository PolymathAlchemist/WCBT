# World Chronicle Backup Tool (WCBT)

This repository is scaffolded to implement the **engine-first** architecture from the WCBT v1.0 specification.

## Quickstart (dev)

This project is designed to be developed with **uv**.

```bash
# Install uv (once, per machine)
# See: https://docs.astral.sh/uv/

cd wcbt
uv venv
uv sync --all-extras

# Run the CLI entry point (placeholder for now)
uv run wcbt --help
```

## Code quality

```bash
uv run ruff check .
uv run ruff format .
uv run mypy .
uv run pytest
```

## Structure

- `backup_engine/` contains the core engine and must never import GUI modules.
- `gui/` will contain the PySide6 UI.
- `tests/` contains unit/integration tests.
