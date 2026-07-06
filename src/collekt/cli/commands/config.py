"""`collekt config` — inspect the merged configuration."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from rich.console import Console

from collekt.core.config import load_config


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``config`` subcommand and its ``show`` action."""
    parser = subparsers.add_parser("config", help="Inspect configuration")
    config_subparsers = parser.add_subparsers(dest="config_command", required=True)
    show = config_subparsers.add_parser("show", help="Print merged configuration")
    show.add_argument("--conf-dir", type=Path)
    show.set_defaults(handler=execute)


def execute(args: argparse.Namespace, console: Console) -> int:
    """Print the merged configuration as YAML."""
    console.print(yaml.safe_dump(load_config(conf_dir=args.conf_dir), sort_keys=False))
    return 0
