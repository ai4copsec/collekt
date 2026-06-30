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
from tqdm import tqdm

from ..core.datasource import DataSource
from ..core.types import Query
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
            query: Query = Query(),
            log_level: str | None = None,
            verbose: bool | None = None,
            limit: int | None = 1000,
            output_dir: Path | None = Path(tempfile.gettempdir()),
            output_filename: str = "skytruth.parquet"
           ) -> list[any]:

        url = CERULEAN_SKYTRUTH_API_SLICK
        # Keep sortby here, to handle encoding issue with ~slick_timestamp
        # Sorting is in descending order - so latests timestamps first
        url += "?sortby=%2Dslick_timestamp"

        parameters = {}
        if limit:
            parameters["limit"] = limit

        if query.region:
            with open(query.region, "r") as f:
                geojson = json.load(f)

                geometries = []
                if geojson.get("type") == "FeatureCollection":
                    for feature in geojson["features"]:
                        geometries.append(shapely.geometry.shape(feature["geometry"]))
                else:
                    geometries.append(shapely.geometry.shape(geojson))

                unified_geometry = shapely.ops.unary_union(geometries)
                wkt_geometry = unified_geometry.simplify(0.005, preserve_topology=True).wkt

            cql_filter = f"S_INTERSECTS(geometry, {wkt_geometry})"
            parameters["filter"] = cql_filter
            parameters["filter-lang"] = "cql2-text"
        elif query.latitude and query.longitude:
            min_max = get_coordinates_min_max(query.latitude, query.longitude, radius_in_km=query.radius)
            # bbox=lon0,lat0,lon1,lat1"
            parameters["bbox"] = f"{min_max['lon_min']},{min_max['lat_min']},{min_max['lon_max']},{min_max['lat_max']}"

        if query.from_time:
            # Format: datetime=START_DATE/END_DATE
            from_time_txt = query.from_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            from_time_txt = ".."

        if query.to_time:
            to_time_txt = query.to_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            to_time_txt = ".."

        if query.from_time or query.to_time:
            parameters["datetime"] = f"{from_time_txt}/{to_time_txt}"

        page_count = 0
        pbar = None
        dataframes = []

        logger.info(f"SkytruthDataset: querying {url} with {parameters}")
        while url:
            page_count += 1
            try:
                response = requests.get(url, params=parameters)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"SkytruthDataset: failed to retrieve data -- {e}")
                break

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
            dataframes.append(pl.from_pandas(gdf.drop(columns=["geometry"])))

            returned = data.get("numberReturned", 0)
            matched = data.get("numberMatched", 0)

            # Initialize the progress bar once we know the total ('matched') count
            if pbar is None and matched > 0:
                pbar = tqdm(total=matched, desc="Downloading records", unit="record")

            # Update the progress bar by the amount of records fetched in this batch
            if pbar and returned > 0:
                pbar.update(returned)

            # Find the 'next' URL in the links array
            next_url = None
            links = data.get("links", [])
            for link in links:
                if link.get("rel") == "next":
                    next_url = link.get("href")
                    break  # Found it, exit the inner loop

            # Update the url variable. If next_url is None, the while loop automatically ends.
            url = next_url

        if pbar:
            pbar.close()

        if not dataframes:
            logger.warning("SkytruthDataset: no matching results")
            return

        df = pl.concat(dataframes)

        m = damast.core.MetaData.load_yaml(CERULEAN_SKYTRUTH_SPEC_YAML)
        m.add_annotation(damast.core.Annotation(name=damast.core.Annotation.Key.Comment, value=f"Created from request: {response.request.url}"))

        adf = damast.core.AnnotatedDataFrame(dataframe=df, metadata=m, validation_mode=damast.core.ValidationMode.UPDATE_DATA)
        filename = output_dir / output_filename
        adf.export(filename)

        logger.info(f"skytruth: saved {filename}")

