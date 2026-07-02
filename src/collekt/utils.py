import json
from pathlib import Path

import geopy.distance as geopy_distance
import shapely.wkt


def get_coordinates_min_max(latitude: float, longitude: float, radius_in_km: float):
    center = (latitude, longitude)

    lat_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=0).latitude
    lat_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=180).latitude

    lon_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=270).longitude
    lon_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=90).longitude

    return {"lat_min": lat_min, "lat_max": lat_max, "lon_min": lon_min, "lon_max": lon_max}


def unified_geometry(geojson):
    geometries = []
    if geojson.get("type") == "FeatureCollection":
        for feature in geojson["features"]:
            geometries.append(shapely.geometry.shape(feature["geometry"]))
    else:
        geometries.append(shapely.geometry.shape(geojson))

    unified_geometry = shapely.ops.unary_union(geometries)
    return unified_geometry.simplify(0.005, preserve_topology=True)


def wkt_from_geojson(geojson: dict[str, any]):
    return unified_geometry(geojson).wkt


def wkt_from_geojson_file(filename: Path | str):
    with open(filename) as f:
        geojson = json.load(f)
        return wkt_from_geojson(geojson)
