"""
Command-line interface for WCBT.

Notes
-----
The CLI is intentionally thin. It parses arguments and delegates to engine modules.

Safety posture (backup command)
-------------------------------
- Default: plan-only (no filesystem writes, no deletion).
- --write-plan: still plan-only, but writes a rendered plan report to disk.
- --materialize: creates a run directory and writes plan.txt + manifest.json.
- --execute: materialize and then copy files (copy-only; no deletion).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from backup_engine.backup.service import run_backup
from backup_engine.errors import WcbtError
from backup_engine.init_profile import init_profile, profile_paths_as_text
from backup_engine.paths_and_safety import SafetyViolationError
from backup_engine.restore.errors import RestoreConflictError
from backup_engine.restore.service import run_restore


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser for the WCBT command-line interface.

    Notes
    -----
    Parsing is intentionally separate from execution. The CLI delegates to engine
    modules so behavior remains testable outside the CLI entrypoint.
    """
    parser = argparse.ArgumentParser(prog="wcbt", description="World Chronicle Backup Tool (WCBT)")

    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser("init-profile", help="Initialize a profile directory structure.")

    init_p.add_argument("--profile", required=True, help="Profile name.")

    init_p.add_argument(
        "--data-root", default=None, help="Override WCBT data root (primarily for testing)."
    )
    init_p.add_argument(
        "--print-paths", action="store_true", help="Print resolved paths after initialization."
    )

    backup_p = sub.add_parser("backup", help="Plan or execute a backup run.")

    backup_p.add_argument("--profile", required=True, help="Profile name.")

    backup_p.add_argument("--source", required=True, type=Path, help="Source directory to back up.")

    backup_p.add_argument(
        "--data-root",
        default=None,
        help="Override WCBT data root (primarily for testing). If omitted, defaults are used.",
    )
    backup_p.add_argument(
        "--exclude-dir",
        action="append",
        default=[],
        help="Directory name to exclude from traversal. Repeatable.",
    )
    backup_p.add_argument(
        "--exclude-file",
        action="append",
        default=[],
        help="File name to exclude from traversal. Repeatable.",
    )
    backup_p.add_argument(
        "--no-default-excludes",
        action="store_true",
        help="Disable built-in default excludes (e.g., .venv, .git).",
    )
    backup_p.add_argument(
        "--max-items",
        type=int,
        default=100,
        help="Maximum number of planned operations to list in output (default: 100).",
    )

    mode = backup_p.add_mutually_exclusive_group(required=False)

    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only (no filesystem writes). This is the default behavior.",
    )
    mode.add_argument(
        "--materialize",
        action="store_true",
        help="Create a run directory and write plan.txt + manifest.json (no copy).",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Materialize and then execute copy operations into the run directory (copy-only; no deletion).",
    )

    backup_p.add_argument(
        "--force",
        action="store_true",
        help="When a profile lock exists and is provably stale (same host, dead PID), break it automatically.",
    )
    backup_p.add_argument(
        "--break-lock",
        action="store_true",
        help="Break an existing profile lock even when it is not provably stale. Use with care.",
    )

    backup_p.add_argument(
        "--write-plan",
        action="store_true",
        help="In plan-only mode, write the rendered plan report to a plan.txt file.",
    )
    backup_p.add_argument(
        "--plan-path",
        type=Path,
        default=None,
        help="In plan-only mode, override the path where plan.txt is written.",
    )
    backup_p.add_argument(
        "--overwrite-plan",
        action="store_true",
        help="In plan-only mode, allow overwriting an existing plan file.",
    )

    verify_p = sub.add_parser("verify", help="Verify a materialized run by hashing archived files.")

    verify_p.add_argument("--profile", required=True, help="Profile name.")

    verify_p.add_argument(
        "--run-id", required=True, help="Run ID to verify (directory name under archives root)."
    )

    verify_p.add_argument(
        "--data-root",
        default=None,
        help="Override WCBT data root (primarily for testing). If omitted, defaults are used.",
    )
    verify_p.add_argument(
        "--force",
        action="store_true",
        help="When a profile lock exists and is provably stale (same host, dead PID), break it automatically.",
    )
    verify_p.add_argument(
        "--break-lock",
        action="store_true",
        help="Break an existing profile lock even when it is not provably stale. Use with care.",
    )

    restore_p = sub.add_parser("restore", help="Plan a restore run from a run manifest.json.")
    restore_p.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="Path to a run manifest.json (schema wcbt_run_manifest_v2).",
    )
    restore_p.add_argument(
        "--dest",
        required=True,
        type=Path,
        help="Destination root to restore into (user-visible paths live here).",
    )
    restore_p.add_argument(
        "--mode",
        choices=["add-only", "overwrite"],
        default="add-only",
        help="Restore mode: add-only (default) or overwrite existing destination files.",
    )
    restore_p.add_argument(
        "--verify",
        choices=["none", "size"],
        default="size",
        help="Verification level for later milestones; recorded in restore plan now.",
    )
    restore_p.add_argument(
        "--data-root",
        default=None,
        help="Override WCBT data root (primarily for testing). If omitted, defaults are used.",
    )
    restore_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan/materialize only (no promotion into destination).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    Run the WCBT CLI.

    Parameters
    ----------
    argv:
        Optional argument vector. If None, arguments are read from sys.argv.

    Returns
    -------
    int
        Exit code.

        - 0: Success
        - 1: Restore failed (non-conflict)
        - 2: Input error, safety violation, domain error, verify/backup failure, or restore conflict
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-profile":
        data_root = Path(args.data_root) if args.data_root else None
        paths = init_profile(profile_name=args.profile, data_root=data_root)
        if args.print_paths:
            print(profile_paths_as_text(paths))
        return 0

    if args.command == "backup":
        data_root = Path(args.data_root) if args.data_root else None

        plan_only = not bool(args.materialize or args.execute)

        if args.write_plan or args.plan_path is not None:
            if not plan_only:
                print(
                    "ERROR: --write-plan/--plan-path are only valid in plan-only mode (omit --materialize/--execute)."
                )
                return 2

        try:
            run_backup(
                profile_name=args.profile,
                source=args.source,
                dry_run=plan_only,
                data_root=data_root,
                excluded_directory_names=args.exclude_dir,
                excluded_file_names=args.exclude_file,
                use_default_excludes=not args.no_default_excludes,
                max_items=args.max_items,
                write_plan=args.write_plan,
                plan_path=args.plan_path,
                overwrite_plan=args.overwrite_plan,
                execute=bool(args.execute),
                force=bool(args.force),
                break_lock=bool(args.break_lock),
            )
        except (SafetyViolationError, WcbtError, ValueError) as exc:
            print(f"ERROR: {exc}")
            return 2
        return 0

    if args.command == "verify":
        from backup_engine.verify import verify_run

        data_root = Path(args.data_root) if args.data_root else None
        try:
            verify_run(
                profile_name=args.profile,
                run_id=args.run_id,
                data_root=data_root,
                force=bool(args.force),
                break_lock=bool(args.break_lock),
            )
        except (WcbtError, ValueError) as exc:
            print(f"ERROR: {exc}")
            return 2
        return 0

    if args.command == "restore":
        data_root = Path(args.data_root) if args.data_root else None
        try:
            run_restore(
                manifest_path=args.manifest,
                destination_root=args.dest,
                mode=args.mode,
                verify=args.verify,
                dry_run=args.dry_run,
                data_root=data_root,
            )
        except RestoreConflictError as exc:
            print(f"ERROR: {exc}")
            return 2
        except (SafetyViolationError, WcbtError, ValueError) as exc:
            print(f"ERROR: {exc}")
            return 1
        return 0

    raise AssertionError(f"Unhandled command: {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
