"""collekt: a library to facilitate spatio-temporal data collection.

Public API.
"""

# Importing the sources package registers the built-in source adapters.
from collekt import sources  # noqa: F401
from collekt.core.fetcher import Fetcher
from collekt.core.request import Region, Request
from collekt.core.result import Result

from .version import __version__, __version_info__

__all__ = [
    "Fetcher",
    "Region",
    "Request",
    "Result",
    "__version__",
    "__version_info__",
]
