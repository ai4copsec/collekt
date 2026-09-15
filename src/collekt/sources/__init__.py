"""Source adapters.

Importing this package registers the built-in adapters (through
`collekt.sources.base.register_adapter`). Each adapter fetches source-native
files for a request and returns `SourceResult` records; the shared contract
lives in `collekt.sources.base`.
"""

# Importing each adapter module registers it. Adapters lazy-import their heavy
# provider clients, so importing this package stays light.
from collekt.sources import (  # noqa: F401
    cmems,
    copernicus_dataspace,
    ecmwf_open_data,
    eodyn,
    era5,
    gfs,
    gfw,
    hozint,
    skytruth,
)
from collekt.sources.base import SourceResult, SourceStatus  # noqa: F401
from collekt.sources.eodyn import eOdynArchiveWarning  # noqa: F401
