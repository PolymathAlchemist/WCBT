"""
Command-line interface for WCBT.

Notes
-----
The CLI is intentionally thin. It parses arguments and delegates to engine modules.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from backup_engine.backup.service import run_backup
from backup_engine.errors import WcbtError
from backup_engine.init_profile import init_profile, profile_paths_as_text
from backup_engine.paths_and_safety import SafetyViolationError


def build_parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="wcbt",
        description="World Chronicle Backup Tool",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser(
        "init",
        help="Initialize a profile's on-disk folder structure (dry-run safe)",
    )
    init_p.add_argument("--profile", required=True, help="Profile name to initialize")
    init_p.add_argument(
        "--data-root",
        default=None,
        help="Override WCBT data root (primarily for testing). If omitted, defaults are used.",
    )
    init_p.add_argument(
        "--print-paths",
        action="store_true",
        help="Print resolved paths after initialization",
    )

    backup_p = sub.add_parser(
        "backup",
        help="Run a backup for a profile (dry-run safe)",
    )
    backup_p.add_argument("--profile", required=True, help="Profile name to back up")
    backup_p.add_argument("--source", required=True, type=Path, help="Source folder to back up")
    backup_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only; do not write or delete backup data",
    )
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

    # Plan artifact output (safe, text-only).
    backup_p.add_argument(
        "--write-plan",
        action="store_true",
        help="Write the rendered plan report to disk.",
    )
    backup_p.add_argument(
        "--plan-path",
        type=Path,
        default=None,
        help="Optional output path for the plan report. Defaults to <archive_root>\\plan.txt.",
    )
    backup_p.add_argument(
        "--overwrite-plan",
        action="store_true",
        help="Allow overwriting an existing plan file.",
    )

    # Placeholders (fine to keep these)
    sub.add_parser("repair-index", help="Rebuild the SQLite index from manifests (not implemented yet)")
    sub.add_parser("restore", help="Restore a backup by ID (not implemented yet)")
    sub.add_parser("sync", help="Replicate backups to configured storage (not implemented yet)")

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        data_root = Path(args.data_root) if args.data_root else None
        paths = init_profile(profile_name=args.profile, data_root=data_root)
        if args.print_paths:
            print(profile_paths_as_text(paths))
        return 0

    if args.command == "backup":
        data_root = Path(args.data_root) if args.data_root else None
        try:
            run_backup(
                profile_name=args.profile,
                source=args.source,
                dry_run=args.dry_run,
                data_root=data_root,
                excluded_directory_names=args.exclude_dir,
                excluded_file_names=args.exclude_file,
                use_default_excludes=not args.no_default_excludes,
                max_items=args.max_items,
                write_plan=args.write_plan,
                plan_path=args.plan_path,
                overwrite_plan=args.overwrite_plan,
            )
        except (SafetyViolationError, WcbtError, ValueError) as exc:
            print(f"ERROR: {exc}")
            return 2
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
