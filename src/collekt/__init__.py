"""collekt: a library to facilitate spatio-temporal data collection.

Public API.
"""

from dotenv import find_dotenv, load_dotenv

# Importing the sources package registers the built-in source adapters.
from collekt import sources  # noqa: F401
from collekt.assemble import Assembler
from collekt.core.datasets import (
    CMEMS,
    ERA5,
    CopernicusDataSpace,
    DatasetConfig,
    ECMWFOpenData,
    Eodyn,
    Hozint,
    SkyTruth,
)
from collekt.core.doctor import DoctorCheck, run_doctor
from collekt.core.fetcher import Fetcher
from collekt.core.request import Region, Request
from collekt.core.result import Result

from .version import __version__, __version_info__

# Load a project-local .env (searched upward from the working directory) so
# credentials placed there reach every provider adapter: the provider tools read
# the OS environment, which this fills. Existing environment variables win.
load_dotenv(find_dotenv(usecwd=True))

__all__ = [
    "Assembler",
    "CMEMS",
    "CopernicusDataSpace",
    "DatasetConfig",
    "DoctorCheck",
    "ECMWFOpenData",
    "ERA5",
    "Eodyn",
    "Fetcher",
    "Hozint",
    "Region",
    "Request",
    "Result",
    "SkyTruth",
    "run_doctor",
    "__version__",
    "__version_info__",
]
