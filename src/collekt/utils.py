import geopy.distance as geopy_distance


def get_coordinates_min_max(latitude: float, longitude: float, radius_in_km: float):
    center = (latitude, longitude)

    lat_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=0).latitude
    lat_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=180).latitude

    lon_min = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=270).longitude
    lon_max = geopy_distance.distance(kilometers=radius_in_km).destination(center, bearing=90).longitude

    return {
            'lat_min': lat_min,
            'lat_max': lat_max,
            'lon_min': lon_min,
            'lon_max': lon_max
    }

