# WCBT Codex Instructions

## Working style

- Make minimal, localized, additive changes.
- Preserve backward compatibility unless explicitly told otherwise.
- Do not perform broad refactors without approval.
- Do not redesign architecture, archive layout, profile behavior, or UI unless explicitly requested.
- Prefer clarity over cleverness.
- Use descriptive variable, parameter, function, and class names.
- Do not introduce cryptic abbreviations such as `mpr`, `cfg`, `tmp`, or similarly compressed names unless they are already established domain terms in this codebase.
- When in doubt, choose the more explicit and readable name.
- When generating or editing code, prefer consistency with surrounding files over introducing a new style.

## Python style requirements

- Follow PEP 8.
- Keep Ruff, mypy, and pytest passing.
- Do not introduce formatting or style changes that would conflict with the current repo tooling.
- Do not adopt Black-specific formatting changes unless explicitly requested and verified to not create friction with Ruff, mypy, or pytest.
- Prefer explicit, maintainable code over compact or clever code.

## Docstrings

- Use NumPy-style docstrings for all new public functions, methods, classes, and any non-trivial private helpers.
- Allowed sections include `Parameters`, `Returns`, `Raises`, and `Notes` when relevant.
- `Notes` may be used to explain why code exists, a non-obvious design choice, a compatibility constraint, or why a certain handling path is necessary.
- Keep section body indentation consistent with standard NumPy style.
- Do not add `Notes` unless it provides meaningful "why" context.

Example format:

def example_function(path: str, overwrite: bool = False) -> str:

    """
    Brief summary.

    Parameters
    ----------
    path : str
        Description of the path parameter.
    overwrite : bool, optional
        Whether an existing target should be overwritten.

    Returns
    -------
    str
        Description of the returned value.

    Raises
    ------
    ValueError
        Raised when the path is invalid.
    
    Notes
    -----
    Optional information explaining why the code exists
    or why it is handled a certain way.
    """

## Comments

- Only add comments where the "why" is not obvious from the code.
- Do not add comments that merely restate "what" the code is doing.
- Prefer improving naming and structure before adding comments.
- Keep comments concise and high signal.

Good comment:
- Explain a non-obvious constraint, workaround, compatibility issue, or design choice.

Bad comment:
- Repeat the code in English.

## Naming

- Use descriptive names that communicate intent clearly.
- Avoid unnecessary abbreviations.
- Favor names like `manifest_path`, `selected_job_id`, `scheduled_task_name`, and `restore_history_runs`.
- Avoid names like `mp`, `jid`, `tsk`, `hist`, or `data` when a more precise name is possible.

## Change discipline

- Before editing, briefly identify the smallest files/functions that need to change.
- Do not touch unrelated files.
- Do not introduce new abstractions unless they clearly reduce complexity and are necessary for the requested change.
- Prefer passing data through existing layers cleanly over inventing parallel paths.

## Verification

After code changes, run these commands from the repo root:

.\.venv\Scripts\python.exe -m pytest -p no:pytest_cov -o addopts=
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m mypy .

- Do not spend time fixing coverage tooling unless explicitly requested.
- If a check fails, fix only issues related to the requested change unless told otherwise.

## Current project guardrails

- Core GUI backup/restore workflow is functionally complete. Preserve it.
- Job identity is persisted in manifests via `job_id` and optional `job_name`. Preserve backward compatibility for legacy manifests without those fields.
- Legacy restore history visibility must continue to work.
- Telemetry, logging systems, performance optimization, archive-layout redesign, profile-model redesign, and UI redesign are currently out of scope unless explicitly requested.

## For future scheduling work

- On Windows, use Task Scheduler, not cron.
- Prefer a thin scheduling backend around `schtasks.exe`.
- Keep scheduling additive and minimal.
- Reuse the existing WCBT CLI and engine flow for scheduled backups instead of creating a separate execution path.