import logging
import tempfile
from pathlib import Path

import copernicusmarine
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..core.datasource import DataSource
from ..core.types import Query
from ..utils import get_coordinates_min_max

logger = logging.getLogger(__name__)

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
        super().__init__(name=f"copernicusmarine__{dataset_id}")

        self.dataset_id = dataset_id
        self.variables = variables

    def execute(self,
            query: Query = Query(),
            region: Path | None = None,
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[any]:

        credentials = Credentials()
        copernicusmarine.login(username=credentials.username, password=credentials.password)

        min_max = get_coordinates_min_max(query.latitude, query.longitude, radius_in_km=query.radius)

        # https://toolbox-docs.marine.copernicus.eu/en/stable/python-interface.html#copernicusmarine.subset
        ds = copernicusmarine.subset(
            dataset_id=self.dataset_id,
            variables=self.variables,
            start_datetime=query.from_time,
            end_datetime=query.to_time,
            minimum_latitude=min_max['lat_min'],
            maximum_latitude=min_max['lat_max'],
            minimum_longitude=min_max['lon_min'],
            maximum_longitude=min_max['lon_max'],
            output_directory=output_dir
        )

        logger.info(f"Downloaded {ds.file_path}")








