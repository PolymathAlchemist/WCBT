# WCBT — Working Copy Backup Tool

WCBT is an **artifact-first, deterministic backup / restore / verify system**.
Its primary goal is not convenience output, but **inspectable truth**: every
operation produces durable artifacts that describe *what was intended*, *what
was executed*, and *what was observed* — even on failure.

Originally motivated by safely backing up and restoring **Minecraft worlds**,
WCBT has been generalized without abandoning those safety constraints.

---

## Core Principles (Non‑Negotiable)

- **Artifact-first behavior**
  - Plans, execution results, and verification results are written to disk.
  - Artifacts are the contract; stdout is incidental.

- **Deterministic execution**
  - No hidden globals
  - Ordered, repeatable output
  - Verification is driven by *expected operations*, not filesystem discovery.

- **Safe failure modes**
  - Failures never destroy state.
  - Restore is **add-only**; conflicts are surfaced as artifacts + exit codes.

- **Strong typing**
  - `Path`, enums, domain types everywhere
  - `mypy` clean

- **Tests assert on artifacts**
  - JSON / JSONL / text artifacts, not console output.

---

## Pipeline Overview

### 1. Backup
- Builds an explicit **plan** of expected operations.
- Executes copy operations deterministically.
- Writes execution artifacts regardless of success or failure.

### 2. Restore
- Applies **add-only semantics**.
- Never overwrites existing files.
- Conflicts are written as artifacts and surfaced via exit codes.

### 3. Verify
Verification validates expected destinations and emits three artifacts:

- `verify_report.json`
  - Machine-readable summary
- `verify_summary.txt`
  - Human-readable report
- `verify_report.jsonl`
  - Deterministic, one-record-per-expected-file stream

Verification always writes artifacts, even on failure.

---

## Verify Classification (v1.1)

Verification currently classifies each expected file as one of:

- `ok` — exists, readable, and matches expected hash (if available)
- `missing` — expected destination does not exist
- `unreadable` — exists but cannot be read or hashed
- `hash_mismatch` — exists and readable, but content differs from expected hash

JSONL records are:
- deterministic
- ordered by expected operations
- suitable for future drift analysis and classification

---

## Exit Codes

- `0` — all verification records are `ok`
- non‑zero — any non‑ok condition detected

Exit codes never suppress artifact generation.

---

## Status

- Backup pipeline: **complete**
- Restore pipeline: **complete**
- Verify pipeline: **complete**
  - JSON, text, and JSONL artifacts
  - Missing, unreadable, and hash mismatch classification
- Tooling gates:
  - `ruff`, `mypy`, `pytest`, coverage all passing

---

## Philosophy

WCBT is intentionally conservative.

It favors:
- correctness over convenience
- inspectability over speed
- safety over automation

If something goes wrong, WCBT’s job is not to guess — it is to **tell the truth,
clearly, and permanently**.

---

## License

License: To be determined
