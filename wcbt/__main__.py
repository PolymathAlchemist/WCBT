"""
Module entrypoint for the WCBT CLI.

This file exists so that `python -m wcbt ...` works consistently in all
environments, including when the console-script wrapper is not installed.

Notes
-----
This module contains no business logic. It delegates to the CLI module.
"""

from __future__ import annotations

from wcbt.cli import main


def _run() -> None:
    """
    Execute the WCBT command line interface.

    Returns
    -------
    None
        This function returns only if the CLI exits normally.

    Raises
    ------
    SystemExit
        Propagated from argparse for normal CLI termination semantics.
    """
    main()


if __name__ == "__main__":
    _run()
