import datetime as dt
import difflib
import json
import logging
import tempfile
from enum import Enum
from pathlib import Path

import requests
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from tqdm import tqdm

from ..core.datasource import DataSource
from ..core.types import Query
from ..utils import get_coordinates_min_max

logger = logging.getLogger(__name__)

COPERNICUS_CATALOG = "https://stac.dataspace.copernicus.eu/v1/"
# https://documentation.dataspace.copernicus.eu/APIs/STAC.html#available-collections
COPERNICUS_COLLECTIONS_URL = COPERNICUS_CATALOG + "collections"

class Credentials(BaseSettings):
    username: str
    password: str

    model_config = SettingsConfigDict(
                    env_file='.env',
                    env_nested_delimiter='__',
                    env_prefix='COPERNICUS_DATASPACE_',
                    extra='ignore'
                )

# https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html
# https://sentiwiki.copernicus.eu/web/s1-products
#
# The is a list of default collections - the actual list should be retrieved
# via CopernicusDataspace.get_collections()
#
class CopernicusCollections(str, Enum):
    LANDSAT_OT_L1      = 'landsat-ot-l1'

    SENTINEL_1_GRD = 'sentinel-1-grd'
    SENTINEL1_SLC = "sentinel-1-slc"

    SENTINEL_2_L1C     = 'sentinel-2-l1c'

    SENTINEL_2_L2A = 'sentinel-2-l2a'
    SENTINEL_3_OLCI    = 'sentinel-3-olci'
    SENTINEL_3_OLCI_L2 = 'sentinel-3-olci-l2'
    SENTINEL_3_SLSTR   = 'sentinel-3-slstr'
    SENTINEL_3_SLSTR_L2 = 'sentinel-3-slstr-l2'
    SENTINEL_3_SYNERGY_L1 = 'sentinel-3-synergy-l2'
    SENTINEL_5P_L2 =  'sentinel-5p-l2'

#    SENTINEL2 = "SENTINEL-2"
#    SENTINEL3 = "SENTINEL-3"
#
#    SENTINEL5 = "SENTINEL-5P"
#    SENTINEL6 = "SENTINEL-6"
#    SENTINEL1RTC = "SENTINEL-1-RTC"
#
#    GLOBAL_MOSAICS = "GLOBAL-MOSAICS"
#    SMOS = "SMOS"
#    ENVISAT = "ENVISAT"
#    Landsat5 = "LANDSAT-5"
#    Landsat7 = "LANDSAT-7"
#    Landsat8 = "LANDSAT-8"
#
#    # Copernicus DEM
#    COP_DEM = "COP-DEM"
#
#    TERRAAQUA = "TERRAAQUA"
#    S2GLC = "S2GLC"
#
    def __str__(self):
        return self.name

    def names() -> list[str]:
        return [x.value for x in CopernicusCollections]

class MinMax(BaseModel):
    lower_bound: int | float
    upper_bound: int | float

    def __repr__(self):
        return f"[{self.lower_bound},{self.upper_bound}]"

class QueryParameters(BaseModel):
    cloudCover: int = Field(None, ge=0, le=10)


def get_datetime(time: str | dt.datetime):
    if type(time) is str:
        dt_time = dt.datetime.fromisoformat(time)
        return dt_time.replace(tzinfo=dt.timezone.utc)

    if type(time) is dt.datetime:
        return time.replace(tzinfo=dt.timezone.utc)

    raise ValueError("Invalid type for time: {time=}, str or datetime required")


class CopernicusDataspace(DataSource):
    # the name of the collection to use
    collection: str
    available_collections: dict[str, str]

    query_parameters: QueryParameters

    access_token: str | None

    AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

    def __init__(self, collection: str, query_parameters: QueryParameters = QueryParameters()):
        super().__init__(name=f"copernicus_{collection}")

        self.available_collections = CopernicusDataspace.get_collections()
        if collection not in self.available_collections:
            closest_matches = difflib.get_close_matches(collection, self.available_collections, n=8, cutoff=0.3)
            raise ValueError(f"Collection {collection} is not known. Did you mean {','.join(closest_matches)}")

        self.collection = collection
        self.query_parameters = query_parameters

        self.access_token = None

    def login(self, username: str | None = None, password: str | None = None):

        if not username and not password:
            credentials = Credentials()

            username = credentials.username
            password = credentials.password

        data = {
            'client_id': 'cdse-public',
            'grant_type': 'password',
            'username': username,
            'password': password
        }

        response = requests.post(self.AUTH_URL, data=data)
        response.raise_for_status()  # Check for errors
        self.access_token = response.json()['access_token']

    @classmethod
    def get_collections(cls, search: str | None = None, access_token: str | None = None, limit: int = 1000) -> dict[str, str]:
        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        params = {}
        if search:
            params['q'] = search

        if limit:
            params['limit'] = limit

        response = requests.get(COPERNICUS_COLLECTIONS_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        collections = {}
        for collection in data.get("collections", []):
            collections[collection.get('id')] = collection.get('description')

        return collections


    # https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html
    def search(self,
            collection: str | None = None,
            start_date: str | dt.datetime| None = None,
            end_date: str | dt.datetime | None = None,
            cloud_cover: MinMax | None = None,
            max_records: int = 100,
            lat: float | None = None,
            lon: float | None = None,
            radius: float | None = None
            ):

        search_path = f"{COPERNICUS_CATALOG}/search"
        params = {}
        if collection:
            params["collections"] = collection

        if start_date:
            start_date = get_datetime(start_date)

        if end_date:
            end_date = get_datetime(end_date)

        # longitude, latitude for bbox
        if lat and lon and radius:
            coordinates = get_coordinates_min_max(latitude=lat, longitude=lon, radius_in_km=radius)
            params["bbox"] = ','.join([str(coordinates[x]) for x in ['lon_min', 'lat_min', 'lon_max', 'lat_max']])

        params["datetime"] = f"{start_date.isoformat(timespec='seconds')}"
        if end_date != start_date:
            params["datetime"] += f"/{end_date.isoformat(timespec='seconds')}"

        if cloud_cover:
            params["query"] = json.dumps({"eo:cloud_cover": {"lte": repr(cloud_cover) }})

        params["sortby"] = "datetime"
        params["limit"] = max_records

        if self.access_token is None:
            raise RuntimeError("CopernicusDataspace: you need to call 'login()' first")

        headers = { 'Authorization': f'Bearer {self.access_token}' }
        logger.info(f"Searching {search_path} with {params=}")
        breakpoint()
        response = requests.get(search_path, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def search_catalogue(
            self,
            bbox: list[float],
            collections: list[str] | None = None,
            start_date: str | None = None,
            end_date: str | None = None,
            page: int = 1,
        ):
        search_path = "https://catalogue.dataspace.copernicus.eu/stac/search"


        params = {}
        if start_date or end_date:
            params["datetime"] = f"{start_date if start_date else ''}/{end_date if end_date else ''}"

        if bbox:
            params["bbox"] = f"{bbox}"

        if collections:
            params["collections"] = ','.join(collections)

        if self.access_token is None:
            raise RuntimeError("CopernicusDataspace: you need to call 'login()' first")

        params["Authorization"] = f"Bearer {self.access_token}"
        logger.info(f"Search catalogue: {search_path} {params=}")
        response = requests.get(search_path, params=params)
        response.raise_for_status()
        return response.json()


    def execute(self,
            query: Query = Query(),
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[any]:

        self.login()

        df = self.search(
                collection=self.collection,
                start_date=query.from_time,
                end_date=query.to_time,
                cloud_cover=self.query_parameters.cloudCover,
                lat=query.latitude,
                lon=query.longitude,
                radius=query.radius
        )

        if not df["features"]:
            print("No results available")

        for feature in df["features"]:
            # feature['assets']['Product']
            # {'href': 'https://download.dataspace.copernicus.eu/odata/v1/Products(0e6e5227-a075-4ef8-b60c-85b17681cdbe)/$value', 'roles': ['data', 'metadata', 'archive'], 'auth:refs': ['oidc'], 'file:size': 1059374322, 'file:checksum': 'd50110b0498ea87aa05f2a786c598c03ade9da', 'file:local_path': 'S1A_IW_GRDH_1SDV_20240130T151936_20240130T152001_052338_06541C_9EB4_COG.SAFE.zip', 'type': 'application/zip', 'title': 'Zipped product'}

            #feature['assets']['vh']['alternate']['https']['href']

            # SLC individual measurement bands
            #band = "iw1-vv"
            #download_url = feature["assets"][band]["alternate"]["https"]["href"]
            # product_name = feature['properties']['_private']['product_name'] + "-" + band + ".tiff"
            # download_url = feature['assets']['product']['href']
            # product_name = feature['properties']['_private']['product_name']

            # sentinel-1-grd
            download_url = feature['assets']['Product']['href']
            product_name = feature['assets']['Product']['file:local_path']

            headers = {}
            headers["Authorization"] = f"Bearer {self.access_token}"

            filename = output_dir / product_name
            print(f"Starting download of {product_name} from {download_url}")
            response = requests.get(download_url, headers=headers, stream=True)

            # Check for authorization or endpoint errors before starting
            if response.status_code != 200:
                print(f"Failed to initiate download. Status code: {response.status_code}")
                print(response.text)
            else:
                # Get total file size from header (fallback to 0 if not provided by server)
                total_size = int(response.headers.get('content-length', 0))

                # Define block/chunk size (8 KB)
                block_size = 1024 * 8

                # Initialize tqdm progress bar
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=product_name[:20] + "...") as progress_bar:
                    with open(filename, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=block_size):
                            if chunk: # Filter out keep-alive new chunks
                                file.write(chunk)
                                progress_bar.update(len(chunk))
