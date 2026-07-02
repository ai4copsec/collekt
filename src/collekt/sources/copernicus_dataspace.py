"""Copernicus Data Space adapter.

Authenticates against the Copernicus Data Space Ecosystem, searches its STAC
catalogue for a collection over the request region and time window, and streams
the matching product files. Uses only HTTP (`requests`); credentials come from
``COPERNICUS_DATASPACE_``-prefixed environment variables.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests
from pydantic_settings import BaseSettings, SettingsConfigDict

from collekt.core.availability import Availability
from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
)

logger = logging.getLogger(__name__)

AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG = "https://stac.dataspace.copernicus.eu/v1/"
SEARCH_URL = CATALOG + "search"


class _Credentials(BaseSettings):
    username: str
    password: str

    model_config = SettingsConfigDict(
        env_file=".env", env_nested_delimiter="__", env_prefix="COPERNICUS_DATASPACE_", extra="ignore"
    )


def _credentials() -> tuple[str, str]:
    creds = _Credentials()
    return creds.username, creds.password


def _login(username: str, password: str) -> str:
    data = {"client_id": "cdse-public", "grant_type": "password", "username": username, "password": password}
    response = requests.post(AUTH_URL, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


def _search(token: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(SEARCH_URL, params=params, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()


def _download(url: str, token: str, path: Path) -> None:
    with requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True) as response:
        response.raise_for_status()
        with open(path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 8):
                if chunk:
                    handle.write(chunk)


def _search_params(request: Request, source: SourceConfig, collection: str) -> dict[str, Any]:
    params: dict[str, Any] = {
        "collections": collection,
        "sortby": "datetime",
        "limit": int(source.raw.get("max_records", 100)),
    }
    start = request.start_datetime.isoformat(timespec="seconds")
    end = request.end_datetime.isoformat(timespec="seconds")
    params["datetime"] = f"{start}/{end}"
    region = request.region
    if region.geometry:
        params["$filter"] = f"OData.CSC.Intersects(area=geography'{region.geometry}')"
    else:
        params["bbox"] = f"{region.west},{region.south},{region.east},{region.north}"
    cloud_cover = source.raw.get("cloud_cover")
    if cloud_cover is not None:
        params["query"] = json.dumps({"eo:cloud_cover": {"lte": cloud_cover}})
    return params


def _format_for(name: str) -> str:
    suffix = Path(name).suffix.lstrip(".").lower()
    return suffix or "unknown"


def fetch_copernicus_dataspace(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Search the Data Space catalogue and download matching product files."""
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    collection = source.raw.get("collection") or source.dataset_id
    dataset_id = collection or source.name
    if not collection:
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message="no collection configured for source")]

    try:
        token = _login(*_credentials())
    except Exception as exc:  # noqa: BLE001 - auth/credential failures are warnings
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message=f"login failed: {exc}")]

    progress(source.name, f"searching {collection}")
    try:
        catalogue = _search(token, _search_params(request, source, collection))
    except requests.exceptions.RequestException as exc:
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message=str(exc))]

    features = catalogue.get("features", [])
    if not features:
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message="no products for this query")]

    results: list[SourceResult] = []
    for feature in features:
        product = (feature.get("assets") or {}).get("Product")
        if not product or not product.get("href"):
            continue
        name = product.get("file:local_path") or f"{feature.get('id', 'product')}"
        path = out_dir / name
        if path.exists() and config.cache.reuse_existing and not config.cache.overwrite:
            results.append(_result(source, SourceStatus.REUSED, dataset_id, path=path, file_format=_format_for(name)))
            continue
        progress(source.name, f"downloading {name}")
        try:
            _download(product["href"], token, path)
        except requests.exceptions.RequestException as exc:
            results.append(_result(source, SourceStatus.SKIPPED, dataset_id, message=str(exc)))
            continue
        results.append(_result(source, SourceStatus.DOWNLOADED, dataset_id, path=path, file_format=_format_for(name)))

    if not results:
        return [_result(source, SourceStatus.SKIPPED, dataset_id, message="no downloadable products for this query")]
    return results


def _result(
    source: SourceConfig,
    status: SourceStatus,
    dataset_id: str,
    *,
    path: Path | None = None,
    message: str | None = None,
    file_format: str = "product",
) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=status,
        path=path,
        dataset_id=dataset_id,
        variables=source.variables,
        message=message,
        format=file_format,
    )


def plan_copernicus_dataspace(
    request: Request, source: SourceConfig, config: Config, request_dir: Path
) -> list[SourceResult]:
    """Plan the Data Space search without authenticating or downloading."""
    collection = source.raw.get("collection") or source.dataset_id or source.name
    details = {
        "provider": "copernicus-dataspace",
        "method": "stac-search",
        "request": _search_params(request, source, collection),
        "availability": Availability("not_checked", "not_checked").as_dict(),
    }
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            dataset_id=collection,
            variables=source.variables,
            format="product",
            details=details,
        )
    ]


register_adapter(
    SourceAdapter(kind="copernicus_dataspace", fetch=fetch_copernicus_dataspace, plan=plan_copernicus_dataspace)
)
