"""
Main argument parser and CLI entry point.
"""
import logging
import sys
import traceback as tb
from argparse import ArgumentParser
from pathlib import Path

from collekt import __version__ as collekt_version
from collekt.cli.base import BaseParser
from collekt.cli.query import QueryParser
from collekt.core.config import (
    LOG_DATE_FORMAT,
    LOG_FORMAT,
    LOG_STYLE,
)

logging.basicConfig(
    format=LOG_FORMAT,
    style=LOG_STYLE,
    datefmt=LOG_DATE_FORMAT,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MainParser(ArgumentParser):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.description = "collekt - collect spatio-temporal data from arbitrary datasources"

        self.add_argument("-w", "--workdir", default=str(Path(".").resolve()))
        self.add_argument("-v", "--verbose", action="store_true")
        self.add_argument("--log-level", type=str, default="INFO", help="Logging level")
        self.add_argument("--version", action="store_true", default=False,
                          help="Show current version of collekt")

        self.subparsers = self.add_subparsers(help='sub-command help')

    def attach_subcommand_parser(self,
                                 subcommand: str,
                                 help: str,
                                 parser_klass: BaseParser
                                 ):
        parser = self.subparsers.add_parser(subcommand, help=help)
        parser_klass(parser=parser)


def run():
    """
    Run the main command line interface
    """
    main_parser = MainParser()
    main_parser.attach_subcommand_parser(subcommand="query",
                                         help="Query the datasources",
                                         parser_klass=QueryParser)

    args, unknown_args = main_parser.parse_known_args()

    if args.version:
        print(f"collekt {collekt_version}")
        sys.exit(0)

    for current_logger in [logging.getLogger(x) for x in logging.root.manager.loggerDict]:
        if current_logger.name.startswith("collekt"):
            current_logger.setLevel(logging.getLevelName(args.log_level))

    if hasattr(args, "active_subparser"):
        try:
            active_subparser = getattr(args, "active_subparser")
            active_subparser.unknown_args  = unknown_args
            active_subparser.execute(args)
        except Exception as e:
            if args.verbose:
                tb.print_exception(e)
            else:
                print(f"\033[91mError: {e}\033[00m")
            sys.exit(1)
    else:
        main_parser.print_help()


if __name__ == "__main__":
    run()
