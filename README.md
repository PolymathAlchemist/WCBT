# World Chronicle Backup Tool (WCBT)

This repository is scaffolded to implement an **engine-first** architecture, based on the WCBT v1.0 specification.

WCBT focuses on **determinism, inspectability, and atomic safety guarantees**. The engine is built so that every operation can be planned, serialized, journaled, and validated before the filesystem is mutated.

---

## Design principles

- **Engine-first separation**
  - `backup_engine/` contains the core engine and must never import GUI modules
  - `gui/` will contain the PySide6 UI (optional extra)
- **Deterministic behavior**
  - Injectable clocks (no hidden globals)
  - Reproducible plans
- **Inspectable artifacts**
  - Plans, candidates, and journals are written to disk
  - No silent operations
- **Atomic safety**
  - Restore promotion uses atomic filesystem renames
  - Existing destinations are preserved, not overwritten in place
- **Explicit invariants**
  - Invalid states fail early with domain-specific errors
  - Dry-run still enforces safety checks

---

## Project status

**Current capabilities:**
- Backup planning and execution
- Restore planning and staging
- Restore dry-run
  - Writes full artifacts
  - Performs zero file copies
  - Skips promotion
- Restore execution (non-dry-run)
  - Stages content
  - Atomically promotes the staged tree into destination
  - Preserves prior destination safely

**Not yet implemented:**
- Complex restore conflict resolution policies
- Profile-oriented restore flows (reserved)
- Network / remote backends
- GUI / UI (scaffolded via optional dependency)

---

## Quickstart (dev)

This project is designed to be developed with **uv**.

```bash
# Install uv (once, per machine)
# See: https://docs.astral.sh/uv/

# From repo root
uv sync --dev

# Run the CLI entry point
uv run wcbt --help
```

### Optional extras

```bash
# Optional extras are defined in pyproject.toml (gui/security/compression).
# Install extras as needed, depending on your uv workflow.
```

---

## CLI usage

```bash
uv run wcbt --help
uv run wcbt backup --help
uv run wcbt restore --help
```

### Restore dry-run (safe and inspectable)

```bash
uv run wcbt restore \
  --manifest run_manifest.json \
  --dest /path/to/restore \
  --dry-run
```

Dry-run will:
- Validate the manifest (schema + invariants)
- Build a deterministic restore plan
- Materialize restore candidates
- Write artifacts and an execution journal
- Perform **no file copies**
- Perform **no destination promotion**

---

## Restore semantics

### Dry-run artifacts

Artifacts are written to:

```text
<destination>/.wcbt_restore/<run_id>/
```

Artifacts include:
- `restore_plan.json`
- `restore_candidates.jsonl`
- `execution_journal.jsonl`

### Non-dry-run staging and promotion

- Stage root:

```text
<destination>.wcbt_stage/<run_id>/stage_root/
```

- Artifacts live **inside the stage** so they survive atomic promotion.
- Promotion is atomic:
  - Existing destination (if present) is preserved as:

```text
.wcbt_restore_previous_<destination_name>_<run_id>
```

  - Stage root is renamed into place.
- There is **no in-place overwrite**.

---

## Code quality

WCBT enforces quality gates locally and in CI:

- **ruff** for linting
- **ruff format** for formatting
- **mypy (strict)** for static typing
- **pytest + pytest-cov** for testing and coverage
- **pre-commit** to prevent “oops I forgot” issues before commits
- **uv audit** (CI) for dependency vulnerability scanning

Run the full local gate suite:

```bash
uv run ruff format .
uv run ruff check .
uv run mypy .
uv run pytest
uv run pre-commit run --all-files
```

---

## Structure

- `backup_engine/` contains the core engine and must never import GUI modules.
- `wcbt/` contains the CLI entry point and wiring.
- `gui/` will contain the PySide6 UI (optional).
- `tests/` contains unit/integration tests.

---

## License

Proprietary (for now). See `LICENSE` for details.
