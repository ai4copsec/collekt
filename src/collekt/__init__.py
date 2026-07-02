"""collekt: a library to facilitate spatio-temporal data collection.

Public API.
"""

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
