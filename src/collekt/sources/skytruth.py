"""Skytruth (Cerulean) oil-slick feature adapter.

Queries the Cerulean slick catalogue for a region and time window and writes the
detections as an annotated Parquet file. This is a per-request feature source: it
emits a single file for the whole request rather than one file per day.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from collekt.core.availability import Availability
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import format_pattern, pattern_values
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

CERULEAN_API_SLICK = "https://api.cerulean.skytruth.org/collections/public.slick_plus/items"
SPEC_YAML = Path(__file__).parent / "skytruth.spec.yaml"
DEFAULT_DATASET_ID = "public.slick_plus"


def _output_path(request: Request, source: SourceConfig, request_dir: Path) -> Path:
    values = pattern_values(
        source=source.name,
        dataset_id=source.dataset_id or DEFAULT_DATASET_ID,
        region=request.region,
        start=request.start_datetime,
        end=request.end_datetime,
        sampling=request.sampling_label,
    )
    return request_dir / source.path / format_pattern(source.filename_pattern, values)


def _query_parameters(request: Request, source: SourceConfig) -> dict[str, Any]:
    params: dict[str, Any] = {}
    limit = source.raw.get("limit", 1000)
    if limit:
        params["limit"] = limit
    region = request.region
    if region.geometry:
        params["filter"] = f"S_INTERSECTS(geometry, {region.geometry})"
        params["filter-lang"] = "cql2-text"
    else:
        params["bbox"] = f"{region.west},{region.south},{region.east},{region.north}"
    start = request.start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    end = request.end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    params["datetime"] = f"{start}/{end}"
    return params


def _fetch_pages(url: str, parameters: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """Return all features across paginated Cerulean responses and the last URL."""
    features: list[dict[str, Any]] = []
    last_url = url
    while url:
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()
        last_url = response.request.url
        features.extend(data.get("features", []))
        next_url = None
        for link in data.get("links", []):
            if link.get("rel") == "next":
                next_url = link.get("href")
                break
        url = next_url
        parameters = {}  # "next" links already carry their own query string
    return features, last_url


def fetch_skytruth(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Query the Cerulean slick catalogue and write detections as Parquet."""
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    output_path = _output_path(request, source, request_dir)
    if output_path.exists() and config.cache.reuse_existing and not config.cache.overwrite:
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.REUSED,
                path=output_path,
                dataset_id=dataset_id,
                variables=source.variables,
                format="parquet",
            )
        ]

    url = (source.raw.get("api_url") or CERULEAN_API_SLICK) + "?sortby=%2Dslick_timestamp"
    parameters = _query_parameters(request, source)
    progress(source.name, "querying the Cerulean slick catalogue")
    try:
        features, request_url = _fetch_pages(url, parameters)
    except requests.exceptions.RequestException as exc:
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message=str(exc),
                format="parquet",
            )
        ]

    if not features:
        return [
            SourceResult(
                source=source.name,
                status=SourceStatus.SKIPPED,
                dataset_id=dataset_id,
                variables=source.variables,
                message="the API returned 0 features for this query",
                format="parquet",
            )
        ]

    _write_parquet(features, output_path, request_url)
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.DOWNLOADED,
            path=output_path,
            dataset_id=dataset_id,
            variables=source.variables,
            format="parquet",
            details={"provider": "skytruth", "features": len(features)},
        )
    ]


def _write_parquet(features: list[dict[str, Any]], output_path: Path, request_url: str) -> None:
    import damast
    import geopandas as gpd
    import polars as pl
    import shapely.geometry

    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    gdf["geometry_geojson"] = gdf["geometry"].apply(
        lambda geom: json.dumps(shapely.geometry.mapping(geom)) if geom else None
    )
    df = pl.from_pandas(gdf.drop(columns=["geometry"]))
    metadata = damast.core.MetaData.load_yaml(SPEC_YAML)
    metadata.add_annotation(
        damast.core.Annotation(name=damast.core.Annotation.Key.Comment, value=f"Created from request: {request_url}")
    )
    adf = damast.core.AnnotatedDataFrame(
        dataframe=df, metadata=metadata, validation_mode=damast.core.ValidationMode.UPDATE_DATA
    )
    adf.export(output_path)


def plan_skytruth(request: Request, source: SourceConfig, config: Config, request_dir: Path) -> list[SourceResult]:
    """Plan the Skytruth query without contacting the catalogue."""
    dataset_id = source.dataset_id or DEFAULT_DATASET_ID
    details = {
        "provider": "skytruth",
        "method": "items",
        "request": _query_parameters(request, source),
        "availability": Availability("not_checked", "not_checked").as_dict(),
    }
    return [
        SourceResult(
            source=source.name,
            status=SourceStatus.PLANNED,
            path=_output_path(request, source, request_dir),
            dataset_id=dataset_id,
            variables=source.variables,
            format="parquet",
            details=details,
        )
    ]


register_adapter(SourceAdapter(kind="skytruth", fetch=fetch_skytruth, plan=plan_skytruth))
