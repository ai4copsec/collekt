"""collekt: a library to facilitate spatio-temporal data collection.

Public API.
"""

from collekt.core.request import Region, Request

from .version import __version__, __version_info__

__all__ = [
    "Region",
    "Request",
    "__version__",
    "__version_info__",
]
