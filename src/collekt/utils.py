"""Transitional shim: geospatial helpers now live in `collekt.core.geo`.

Kept so the legacy datasources keep importing from here until they are ported to
the new source-adapter framework (see docs/plan.qmd, phase 3). Remove once
nothing imports `collekt.utils`.
"""

from collekt.core.geo import (  # noqa: F401
    get_coordinates_min_max,
    unified_geometry,
    wkt_from_geojson,
    wkt_from_geojson_file,
)
