"""collekt CLI subcommands.

Each command module exposes ``register(subparsers)`` — which adds its parser and
wires its handler via ``set_defaults(handler=...)`` — and a handler
``execute(args, console) -> int``. ``main`` builds the parser by registering every
command in ``COMMANDS`` and dispatches through ``args.handler``.
"""

from collekt.cli.commands import config, doctor, fetch

COMMANDS = (fetch, doctor, config)

__all__ = ["COMMANDS", "config", "doctor", "fetch"]
