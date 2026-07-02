"""Geospatial helpers: bounding-box maths and GeoJSON geometry handling."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopy.distance as geopy_distance
import shapely.geometry
import shapely.ops
import shapely.wkt


def get_coordinates_min_max(latitude: float, longitude: float, radius_in_km: float) -> dict[str, float]:
    """Return the bounding box around a point for a given radius in kilometres."""
    center = (latitude, longitude)

    lat_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=0).latitude
    lat_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=180).latitude

    lon_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=270).longitude
    lon_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=90).longitude

    return {
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_min": lon_min,
        "lon_max": lon_max,
    }


def unified_geometry(geojson: dict[str, Any]):
    """Merge a GeoJSON object into a single simplified shapely geometry."""
    geometries = []
    if geojson.get("type") == "FeatureCollection":
        for feature in geojson["features"]:
            geometries.append(shapely.geometry.shape(feature["geometry"]))
    else:
        geometries.append(shapely.geometry.shape(geojson))

    merged = shapely.ops.unary_union(geometries)
    return merged.simplify(0.005, preserve_topology=True)


def wkt_from_geojson(geojson: dict[str, Any]) -> str:
    """Return the WKT representation of a GeoJSON object."""
    return unified_geometry(geojson).wkt


def wkt_from_geojson_file(filename: Path | str) -> str:
    """Return the WKT representation of a GeoJSON file."""
    with open(filename, encoding="utf-8") as f:
        geojson = json.load(f)
        return wkt_from_geojson(geojson)
