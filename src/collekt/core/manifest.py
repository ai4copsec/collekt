"""Manifest building and post-fetch file inspection.

The manifest is collekt's machine-readable provenance record for a collection
run. It stays intentionally flexible during alpha; a permissive draft schema is
shipped at ``collekt/resources/schema/manifest.schema.json`` for reference but is
not enforced.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.core.reporting import Summary
from collekt.sources.base import SourceResult, SourceStatus

MANIFEST_SCHEMA_VERSION = "0.1"


def inspect_path(path: Path, file_format: str) -> dict[str, Any]:
    """Describe a downloaded file for the manifest.

    Records the file size, and for NetCDF/Parquet the variables/columns and
    dimensions/row count. Missing optional readers degrade to a size-only record
    rather than failing the run.
    """
    if not path.exists():
        return {"status": "missing"}
    inspection: dict[str, Any] = {"status": "ok", "size_bytes": path.stat().st_size}
    fmt = file_format.lower()
    if fmt == "netcdf":
        return _inspect_netcdf(path, inspection)
    if fmt == "parquet":
        return _inspect_parquet(path, inspection)
    return inspection


def _inspect_netcdf(path: Path, inspection: dict[str, Any]) -> dict[str, Any]:
    try:
        import xarray as xr
    except ImportError:
        inspection["status"] = "unavailable"
        inspection["message"] = "xarray is not installed"
        return inspection
    try:
        with xr.open_dataset(path) as ds:
            inspection["data_variables"] = sorted(str(name) for name in ds.data_vars)
            inspection["sizes"] = {str(name): int(size) for name, size in ds.sizes.items()}
            inspection["coordinates"] = sorted(str(name) for name in ds.coords)
    except Exception as exc:  # noqa: BLE001 - corrupted/provider files should be recorded, not fatal
        inspection["status"] = "failed"
        inspection["message"] = str(exc)
    return inspection


def _inspect_parquet(path: Path, inspection: dict[str, Any]) -> dict[str, Any]:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return inspection
    try:
        metadata = pq.read_metadata(str(path))
        inspection["num_rows"] = int(metadata.num_rows)
        inspection["columns"] = list(metadata.schema.names)
    except Exception as exc:  # noqa: BLE001 - corrupted/provider files should be recorded, not fatal
        inspection["status"] = "failed"
        inspection["message"] = str(exc)
    return inspection


def with_inspection(results: tuple[SourceResult, ...]) -> tuple[SourceResult, ...]:
    """Attach inspection metadata to downloaded/reused results."""
    inspected = []
    for result in results:
        if result.path is not None and result.status in {SourceStatus.DOWNLOADED, SourceStatus.REUSED}:
            inspected.append(replace(result, inspection=inspect_path(result.path, result.format)))
        else:
            inspected.append(result)
    return tuple(inspected)


def _result_to_manifest(result: SourceResult, request_dir: Path) -> dict[str, Any]:
    path = None
    if result.path is not None:
        try:
            path = str(result.path.relative_to(request_dir))
        except ValueError:
            path = str(result.path)
    return {
        "source": result.source,
        "dataset_id": result.dataset_id,
        "path": path,
        "status": result.status.value,
        "format": result.format,
        "variables": list(result.variables),
        "day": result.day,
        "message": result.message,
        "details": result.details,
        "inspection": result.inspection,
    }


def _source_to_manifest(source: SourceConfig) -> dict[str, Any]:
    return {
        "kind": source.kind,
        "enabled": source.enabled,
        "variable_groups": list(source.variable_groups),
        "use_variables": list(source.use_variables),
        "default_variables": list(source.default_variables),
        "optional_variables": {name: list(values) for name, values in source.optional_variables.items()},
        "resolved_variables": list(source.variables),
        "temporal": {
            "native_sampling": source.temporal.native_sampling,
            "supported_sampling": list(source.temporal.supported_sampling),
            "sampling_mode": source.temporal.sampling_mode,
        },
    }


def build_manifest(
    *,
    request,
    config: Config,
    preset: str | None,
    request_dir: Path,
    results: tuple[SourceResult, ...],
    summary: Summary,
) -> dict[str, Any]:
    """Build the manifest mapping for a collection run."""
    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "request": request.as_dict() | {"preset": preset},
        "summary": summary.as_dict(),
        "sources": {name: _source_to_manifest(source) for name, source in config.sources.items()},
        "files": [
            _result_to_manifest(result, request_dir)
            for result in results
            if result.status in {SourceStatus.DOWNLOADED, SourceStatus.REUSED}
        ],
        "planned": [
            _result_to_manifest(result, request_dir) for result in results if result.status == SourceStatus.PLANNED
        ],
        "warnings": [
            _result_to_manifest(result, request_dir) for result in results if result.status == SourceStatus.SKIPPED
        ],
        "failures": [
            _result_to_manifest(result, request_dir) for result in results if result.status == SourceStatus.FAILED
        ],
    }


def write_manifest(
    *,
    request,
    config: Config,
    preset: str | None,
    request_dir: Path,
    manifest_path: Path,
    results: tuple[SourceResult, ...],
    summary: Summary,
) -> None:
    """Write the manifest for a collection run to ``manifest_path``."""
    manifest = build_manifest(
        request=request,
        config=config,
        preset=preset,
        request_dir=request_dir,
        results=results,
        summary=summary,
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
