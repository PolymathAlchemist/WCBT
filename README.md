# WCBT (Working Copy Backup Tool)

WCBT is a deterministic, artifact-first backup and restore tool designed for **inspectability, safety, and testability**.  
Every major operation produces on-disk artifacts that describe *what was planned*, *what executed*, and *what was verified*.

This repository is under active development and follows a strict, test-driven integration approach.

---

## Key Principles

- **Artifact-first design**: all critical actions write inspectable artifacts to disk
- **Deterministic behavior**: no hidden globals, no implicit state
- **Safe failure modes**: failures still produce artifacts for post-mortem inspection
- **Add-only restore semantics**: restores never overwrite existing data
- **Strong typing**: `Path`, enums, domain types; `mypy` enforced
- **Minimal cleverness**: readability over compactness

---

## Current Capabilities

### Backup
- End-to-end backup pipeline
- Deterministic plan generation
- Executed copy operations recorded in a manifest
- Dry-run support with artifact output
- Locking to prevent concurrent runs

### Restore
- End-to-end restore pipeline
- Add-only conflict policy (artifact-first failure)
- Restore summary artifacts
- Explicit exit codes for conflict conditions

### Verify
- End-to-end verification of archived runs
- Verifies archived payload files against expected state
- **Artifacts written on both success and failure**

Verify produces **three artifacts** in the run directory:

| Artifact | Description |
|--------|-------------|
| `verify_report.json` | Aggregate verification summary (counts, algorithm, schema) |
| `verify_summary.txt` | Human-readable verification summary |
| `verify_report.jsonl` | One JSON object per verified file (machine-readable stream) |

---

## Verify JSONL Format

`verify_report.jsonl` contains one record per expected file:

```json
{
  "schema": "wcbt_verify_record_v1",
  "run_id": "2025-01-01T12-00-00Z",
  "status": "ok | missing",
  "path": "nested/example.bin"
}
```

Characteristics:
- One JSON object per line (JSONL)
- Deterministic ordering
- Paths are relative to the run root
- Records are emitted **even when verification fails**

This stream is intended for downstream tooling, auditing, and future drift classification.

---

## CLI Overview

```bash
wcbt backup  --profile NAME --source PATH [--dry-run]
wcbt restore --profile NAME --run-id ID
wcbt verify  --profile NAME --run-id ID
```

Exit codes are explicit and stable.

---

## Development Standards

- `ruff`, `pytest`, and `mypy` must pass before commit
- NumPy-style docstrings required for public APIs
- Minimal diffs preferred over rewrites
- File-level anchors required for cross-module changes
- Tests must assert on **artifacts**, not console output

---

## Project Status

Completed milestones:
- Backup pipeline end-to-end
- Restore pipeline end-to-end
- Dry-run artifact contract
- Add-only restore conflict policy
- Restore summary artifacts
- CLI exit codes
- Verify artifacts on success and failure
- Verify JSONL stream with missing-file detection

Next milestones are being planned.
