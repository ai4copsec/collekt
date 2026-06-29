import datetime as dt
import json
import logging
import tempfile
from pathlib import Path

import damast
import geopandas as gpd
import polars as pl
import requests
import shapely.geometry
import shapely.wkt

from ..core.datasource import DataSource
from ..utils import get_coordinates_min_max

logger = logging.getLogger(__name__)

CERULEAN_SKYTRUTH_API_BASE = "https://api.cerulean.skytruth.org"
# This is the endpoint for the slick data
CERULEAN_SKYTRUTH_API_SLICK = CERULEAN_SKYTRUTH_API_BASE + "/collections/public.slick_plus/items"

# Metadata descriptions at https://colab.research.google.com/drive/1SYWIzPH_2NeqvfqcVzY43y1hv1Ug-YoJ#scrollTo=p_j_cNCnqAuQ"
CERULEAN_SKYTRUTH_SPEC_YAML = Path(__file__).parent / "skytruth.spec.yaml"

class SkytruthDataset(DataSource):

    def __init__(self):
        super().__init__(name="skytruth")

    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            log_level: str | None = None,
            verbose: bool | None = None,
            limit: int | None = 1000,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[any]:

        url = CERULEAN_SKYTRUTH_API_SLICK
        # Keep sortby here, to handle encoding issue with ~slick_timestamp
        # Sorting is in descending order - so latests timestamps first
        url += "?sortby=%2Dslick_timestamp"

        parameters = {}
        if limit:
            parameters["limit"] = limit

        if latitude and longitude:
            min_max = get_coordinates_min_max(latitude, longitude, radius_in_km=radius)
            # bbox=lon0,lat0,lon1,lat1"
            parameters["bbox"] = f"{min_max['lon_min']},{min_max['lat_min']},{min_max['lon_max']},{min_max['lat_max']}"

        if from_time:
            # Format: datetime=START_DATE/END_DATE
            from_time_txt = from_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            from_time_txt = ".."

        if to_time:
            to_time_txt = to_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            to_time_txt = ".."

        if from_time or to_time:
            parameters["datetime"] = f"{from_time_txt}/{to_time_txt}"

        logger.info(f"Querying skytruth: {url} with {parameters}")
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()

        if "features" in data and len(data["features"]) == 0:
            logger.warning("SkytruthDataset: the API returned 0 features for this query area.")
            return

        crs = "EPSG:4326"
        if "features" in data:
            gdf = gpd.GeoDataFrame.from_features(data["features"], crs=crs)
        else:
            gdf = gpd.GeoDataFrame.from_features([data], crs=crs)


        gdf["geometry_geojson"] = gdf["geometry"].apply(
            lambda geom: json.dumps(shapely.geometry.mapping(geom)) if geom else None
        )
        df = pl.from_pandas(gdf.drop(columns=["geometry"]))

        m = damast.core.MetaData.load_yaml(CERULEAN_SKYTRUTH_SPEC_YAML)
        m.add_annotation(damast.core.Annotation(name=damast.core.Annotation.Key.Comment, value=f"Created from request: {response.request.url}"))

        adf = damast.core.AnnotatedDataFrame(dataframe=df, metadata=m)
        adf.export(output_dir / "skytruth.parquet")

