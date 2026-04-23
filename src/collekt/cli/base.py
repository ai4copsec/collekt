"""
Module containing the BaseParser functionality, in order to simplify the usage of subparsers.
"""
import argparse
from abc import ABC, abstractmethod
from argparse import ArgumentParser


class BaseParser(ABC):

    @abstractmethod
    def __init__(self, parser: ArgumentParser):
        parser.add_argument('--active_subparser',
                            default=self,
                            action='store',
                            help=argparse.SUPPRESS)

    def execute(self, args):
        print(f"Subparser: {args.active_subparser.__class__.__name__}")
        pass
