"""`collekt doctor` — check the local environment and source configuration."""

from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console
from rich.table import Table

from collekt.core.doctor import run_doctor


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``doctor`` subcommand."""
    parser = subparsers.add_parser("doctor", help="Check the local environment and source configuration")
    parser.add_argument("--conf-dir", type=Path)
    parser.add_argument("--online", action="store_true", help="Validate CMEMS datasets and variables online")
    parser.set_defaults(handler=execute)


def execute(args: argparse.Namespace, console: Console) -> int:
    """Run the diagnostics and render them; return 1 if any check failed."""
    checks = run_doctor(conf_dir=args.conf_dir, online=args.online)
    table = Table(title="collekt doctor")
    table.add_column("Status")
    table.add_column("Check")
    table.add_column("Message")
    status_styles = {"ok": "green", "warn": "yellow", "fail": "red"}
    for check in checks:
        table.add_row(f"[{status_styles.get(check.status, 'white')}]{check.status}[/]", check.name, check.message)
    console.print(table)
    return 1 if any(check.status == "fail" for check in checks) else 0
