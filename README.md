# WCBT — Working Copy Backup Tool

WCBT is an **artifact-first, deterministic backup / restore / verify system**.
Its primary goal is not convenience output, but **inspectable truth**: every
operation produces durable artifacts that describe *what was intended*, *what
was executed*, and *what was observed* — even on failure.

Originally motivated by safely backing up and restoring **Minecraft worlds**,
WCBT has been generalized without abandoning those safety constraints.

---

## What WCBT Is (and Is Not)

**WCBT is:**
- A correctness-first backup and restore engine
- Deterministic and test-driven
- Designed for inspection, auditability, and recovery confidence

**WCBT is not:**
- A convenience wrapper around `rsync`
- A silent or "best-effort" backup tool
- A UI-first consumer application (yet)

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

## Repository Layout (High-Level)

```
wcbt/
├── backup_engine/        # Core backup / restore / verify engine
│   ├── backup/
│   ├── restore/
│   ├── verify/
│   └── data_models.py
├── gui/                  # GUI (PySide) — engine-backed, under active development
│   ├── tabs/
│   ├── dialogs/
│   └── adapters/
├── tests/                # Artifact-driven test suite
├── wcbt/                 # CLI entrypoints
└── README.md
```

---

## Installation

### Requirements

- Python **3.11+**
- `uv` (recommended) or `pip`

### Clone

```bash
git clone https://github.com/PolymathAlchemist/WCBT.git
cd WCBT
```

### Install dependencies (recommended via uv)

```bash
uv sync --dev
```

Alternatively (pip):

```bash
pip install -r requirements.txt
```

---

## Running WCBT (CLI)

WCBT is currently CLI-first.

### Backup

```bash
uv run wcbt backup <source> <destination>
```

### Restore

```bash
uv run wcbt restore <run-directory> <destination>
```

Restore is **add-only**. Existing files are never overwritten.
Conflicts are written as artifacts and surfaced via exit codes.

### Verify

```bash
uv run wcbt verify <run-directory>
```

Verification always produces artifacts, even on failure.

---

## Verification Artifacts

Verification emits three deterministic artifacts:

- `verify_report.json`
  - Machine-readable summary
- `verify_report.jsonl`
  - One record per expected file (ordered, deterministic)
- `verify_summary.txt`
  - Human-readable summary

### Verification Status Values

Each JSONL record includes a `status` field:

- `ok` — exists, readable, and matches expected hash (if available)
- `missing` — expected destination does not exist
- `unreadable` — exists but cannot be read or hashed
- `hash_mismatch` — exists and readable, but content differs

### Exit Codes

- `0` — all records are `ok`
- non-zero — any non-`ok` status detected

Exit codes **never suppress artifact generation**.

---

## GUI Status

A PySide-based GUI is under active development.

Current state:
- Authoring, Run, and Restore tabs are **engine-backed**
- Job lifecycle and rule authoring persist via SQLite (`ProfileStore`)
- Backup and restore execution use engine services directly (no CLI calls)
- Restore history is discovered from real on-disk artifacts
- GUI execution path is fully de-mocked
- The GUI is intended as a structured front-end to the same artifact-driven engine,
not a separate execution layer.

The GUI is not yet considered stable for end users.

To run the GUI (development):

```bash
uv run python -m gui.app
```

---

## Development Gates

Before committing changes, run:

```bash
uv run ruff format .
uv run ruff check .
uv run mypy .
uv run pytest -q
```

The project treats green gates as a hard requirement.

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

WCBT is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This license allows use, study, and modification of the code, while requiring
that derivative works and network-deployed uses remain open and attributable.

The intent of this license is to prevent proprietary appropriation or
unattributed reuse of the project.

For alternative licensing or commercial arrangements, contact the author.
