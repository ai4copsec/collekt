import datetime as dt
import logging
import tempfile
from pathlib import Path

import copernicusmarine
import geopy.distance as geopy_distance
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

    def __init__(self, dataset_id: str, variables: list[str] | None = None):
        super().__init__(name="copernicusmarine")

        self.dataset_id = dataset_id
        self.variables = variables

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
           ) -> list[any]:

        credentials = Credentials()
        copernicusmarine.login(username=credentials.username, password=credentials.password)

        min_max = get_coordinates_min_max(latitude, longitude, radius_in_km=radius)

        # https://toolbox-docs.marine.copernicus.eu/en/stable/python-interface.html#copernicusmarine.subset
        ds = copernicusmarine.subset(
            dataset_id=self.dataset_id,
            variables=self.variables,
            start_datetime=from_time,
            end_datetime=to_time,
            minimum_latitude=min_max['lat_min'],
            maximum_latitude=min_max['lat_max'],
            minimum_longitude=min_max['lon_min'],
            maximum_longitude=min_max['lon_max'],
            output_directory=output_dir
        )

        logger.info(f"Downloaded {ds.file_path}")








