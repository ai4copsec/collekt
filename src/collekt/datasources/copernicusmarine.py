from __future__ import annotations

import datetime as dt
import logging
import tempfile
from pathlib import Path

import copernicusmarine
import geopy.distance as geopy_distance
import pandas as pd
import xarray as xr
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..core.datasource import DataSource

logger = logging.getLogger(__name__)

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

class Credentials(BaseSettings):
    username: str
    password: str

    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='__',
                    env_prefix='COPERNICUSMARINE_SERVICE_',
                    extra='ignore'
                )

class CopernicusMarineDataset(DataSource):
    dataset_id: str
    variables: list[str] | None

    def __init__(self, dataset_id: str, variables: list[str] | None = None,
                 minimum_depth: float = 0.0, maximum_depth: float = 1.0):
        # store the dataset here
        super().__init__(name="copernicusmarine")

        self.dataset_id = dataset_id
        self.variables = variables
        self.minimum_depth = minimum_depth
        self.maximum_depth = maximum_depth

    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[Path]:

        # some validation step of the input-arguments go here. Required -> ValueError
        min_max = get_coordinates_min_max(latitude, longitude, radius_in_km=radius)
        filename = None # filename = _subset_filename(...) --> cache hit? return [path] without download-step
        credentials = Credentials()  # noqa: F841

        # Will try and use Credentials() instead of copernicusmarine.login(username=credentials.username, password=credentials.password)

        response = copernicusmarine.subset(
            dataset_id=self.dataset_id,
            variables=self.variables,
            start_datetime=from_time,
            end_datetime=to_time,
            minimum_depth=self.minimum_depth,
            maximum_depth=self.maximum_depth,
            minimum_latitude=min_max['lat_min'],
            maximum_latitude=min_max['lat_max'],
            minimum_longitude=min_max['lon_min'],
            maximum_longitude=min_max['lon_max'],
            output_filename=filename,
            output_directory=output_dir,
        )


        # https://toolbox-docs.marine.copernicus.eu/en/stable/python-interface.html#copernicusmarine.subset


        logger.info(f"Downloaded {response.file_path}")

        return [Path(response.file_path)]

    def enrich(self, points: pd.DataFrame, output_dir: Path = ...,
               method: str = "linear",
               time_col: str = "timestamp", lat_col: str="latitude", lon_col: str = "longitude") -> pd.DataFrame:

        # window =
        # retrieve-or-reuse subset (cached or not)
        # return interpolate_to_points(subset_path, points)
        raise NotImplementedError


def _subset_filename(dataset_id: str, bbox: dict, from_time: dt.datetime,
                     to_time: dt.datetime, variables: list[str] | None) -> str:
    pass
    return None

def interpolate_to_points(subset: Path | xr.Dataset, points: pd.DataFrame,
                          variables: list[str] | None = None, method: str = "linear",
                          time_col: str = "timestamp", lat_col: str = "latitude",
                          lon_col: str = "longitude") -> pd.DataFrame:
    # open_dataset if Path; .isel(depth=0) iff "depth" in ds.dims;
    # reuse interpolation-function from argo-eke
    # OR:
    # ds.interp(latitude=xr.DataArray(lats, dims="points"),
    #           longitude=xr.DataArray(lons, dims="points"),
    #           time=xr.DataArray(times, dims="points"), method=method)
    # -> returns points.copy() one new float column per variable
    raise NotImplementedError











