"""Command-line interface for collekt.

``main`` is a thin orchestrator: it builds the parser by registering every command
in `collekt.cli.commands.COMMANDS`, then dispatches to the handler each command
wires via ``set_defaults(handler=...)``. Command-specific arguments and rendering
live in the command modules.
"""

from __future__ import annotations

import argparse
import logging

from rich.console import Console

from collekt.cli.commands import COMMANDS
from collekt.core.config import LOG_DATE_FORMAT, LOG_FORMAT, LOG_STYLE

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the collekt argument parser from the registered commands."""
    parser = argparse.ArgumentParser(prog="collekt", description="Collect spatio-temporal data from arbitrary sources")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in COMMANDS:
        command.register(subparsers)
    return parser


def run(argv: list[str] | None = None) -> int:
    """Run the collekt command-line interface."""
    logging.basicConfig(format=LOG_FORMAT, style=LOG_STYLE, datefmt=LOG_DATE_FORMAT, level=logging.INFO)
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args, Console())
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        # Expected fatal errors (missing/invalid config, region, or dataset, or a
        # strict-mode failure) become a logged error and a non-zero exit, never a
        # traceback.
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
