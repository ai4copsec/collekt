"""Multi-day grid retrievals with the original daily output contract."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import ProgressCallback, SourceResult, SourceStatus, get_adapter
from collekt.sources.batching.common import batch_details, cached, failed, staged_output


def daily_groups(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    batch_days: int,
    *,
    compatible: Callable[[SourceResult], Any] = lambda item: None,
) -> tuple[list[SourceResult], list[list[int]]]:
    """Group consecutive missing days within fixed windows and compatible payloads."""
    results = [cached(item, config) for item in get_adapter(source.kind).plan(request, source, config, request_dir)]
    paths = [item.path for item in results if item.path is not None]
    if len(paths) != len(set(paths)):
        raise ValueError("batch downloads require a distinct output filename for each day")
    groups = []
    previous = None
    for index, item in enumerate(results):
        if item.status != SourceStatus.PLANNED or item.day is None:
            previous = None
            continue
        day = date.fromisoformat(item.day)
        bucket = (day - request.start_datetime.date()).days // batch_days
        key = (bucket, item.dataset_id, compatible(item))
        if previous is None or previous[0] != key or day != previous[1] + timedelta(days=1):
            groups.append([])
        groups[-1].append(index)
        previous = key, day
    return results, groups


def _compatibility(kind: str, item: SourceResult) -> dict[str, Any]:
    body = dict(item.details["request"])
    if kind == "cmems" and body["coordinates_selection_method"] not in {"outside", "nearest"}:
        # Inside selections fall back to a nearest neighbour when empty. A
        # range subset can exclude that neighbour at either end; without the
        # provider's complete time index, keep those selections independent.
        return body
    for key in {"era5": ("day",), "cmems": ("start_datetime", "end_datetime"), "ecmwf_open_data": ("date",)}[kind]:
        body.pop(key, None)
    return body


def _payload(kind: str, members: list[SourceResult]) -> dict[str, Any]:
    body = dict(members[0].details["request"])
    if kind == "era5":
        body["day"] = [day for item in members for day in item.details["request"]["day"]]
        if body["data_format"] != "netcdf" or body["download_format"] != "unarchived":
            raise ValueError("ERA5 batch downloads require data_format='netcdf' and download_format='unarchived'")
    elif kind == "ecmwf_open_data":
        body["date"] = [item.details["request"]["date"] for item in members]
    else:
        body["end_datetime"] = members[-1].details["request"]["end_datetime"]
    return body


def run_grid_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    batch_days: int,
    dry_run: bool,
    progress: ProgressCallback,
) -> list[SourceResult]:
    """Plan and execute identical provider groups, splitting each into daily files."""
    results, groups = daily_groups(
        request,
        source,
        config,
        request_dir,
        batch_days,
        compatible=lambda item: _compatibility(source.kind, item),
    )
    for indices in groups:
        members = [results[index] for index in indices]
        payload = _payload(source.kind, members)
        batch = batch_details(
            source,
            members[0].day,
            members[-1].day,
            payload,
            transport="individual_files" if source.kind == "ecmwf_open_data" else "range",
        )
        for index in indices:
            results[index] = replace(results[index], details=results[index].details | {"batch": batch})
        if dry_run:
            continue
        progress(source.name, f"downloading batch {batch['start']} to {batch['end']}")
        out_dir = request_dir / source.path
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            with TemporaryDirectory(prefix=".collekt-batch-", dir=out_dir) as directory:
                path = Path(directory) / "batch.nc"
                message = _retrieve(request, source, payload, path)
                import xarray as xr

                with xr.open_dataset(path) as dataset:
                    for index in indices:
                        item = results[index]
                        try:
                            daily = (
                                dataset
                                if len(indices) == 1 and source.kind == "cmems"
                                else _split(dataset, item, source.kind)
                            )
                            daily = _prepare_daily(daily)
                            with staged_output(item.path) as staged:
                                daily.to_netcdf(staged)
                            results[index] = replace(item, status=SourceStatus.DOWNLOADED, message=message)
                        except Exception as exc:  # noqa: BLE001 - isolate incomplete daily slices
                            results[index] = failed(item, exc)
        except Exception as exc:  # noqa: BLE001 - one unavailable batch must not block the next
            for index in indices:
                if results[index].status == SourceStatus.PLANNED:
                    results[index] = failed(results[index], exc)
    return results


def _retrieve(request: Request, source: SourceConfig, payload: dict[str, Any], path: Path) -> str | None:
    if source.kind == "era5":
        from collekt.sources import era5

        body = dict(payload)
        dataset_id = body.pop("dataset_id")
        era5._cdsapi().Client().retrieve(dataset_id, body).download(str(path))
    elif source.kind == "cmems":
        from collekt.sources import cmems

        cmems._copernicusmarine().subset(
            **payload,
            output_filename=path.name,
            output_directory=str(path.parent),
            overwrite=True,
            disable_progress_bar=True,
        )
    else:
        from collekt.sources import ecmwf_open_data as ecmwf

        resol = str(source.raw.get("resol", "0p25"))
        client = ecmwf._client_class()(
            source=str(source.raw.get("source", "ecmwf")),
            model=str(source.raw.get("model", "ifs")),
            resol=resol,
        )
        raw = path.with_suffix(".grib2")
        client.retrieve(payload, target=str(raw))
        missing = ecmwf._crop_to_netcdf(
            raw,
            path,
            request.region,
            float(source.raw.get("pad_deg", ecmwf.DEFAULT_PAD_DEG)),
            ecmwf._grid_resolution(resol),
            source.variables,
        )
        if missing:
            return f"missing from the cropped file (provider limitation): {', '.join(missing)}"
    return None


def _split(dataset: Any, item: SourceResult, kind: str) -> Any:
    import numpy as np

    payload = item.details["request"]
    if kind == "ecmwf_open_data":
        # `time` is the run, whereas `valid_time` combines run and lead time.
        hour = int(str(payload["time"]).split(":", 1)[0])
        run = np.datetime64(f"{item.day}T{hour:02d}:00:00")
        if "time" in dataset.dims:
            return dataset.sel(time=run)
        if dataset.coords["time"].values != run:
            raise ValueError(f"missing forecast run {run}")
        return dataset
    if kind == "cmems":
        return _cmems_slice(dataset, payload)
    coordinate = "valid_time" if "valid_time" in dataset.coords else "time"
    expected = np.array([f"{item.day}T{hour}" for hour in payload["time"]], dtype="datetime64[ns]")
    if not np.isin(expected, dataset[coordinate].values).all():
        raise ValueError(f"batch is missing requested timestamps for {item.day}")
    if dataset[coordinate].ndim != 1:
        if len(expected) == 1 and dataset[coordinate].values == expected[0]:
            return dataset
        raise ValueError("ERA5 batch requires a one-dimensional time coordinate")
    return dataset.sel({coordinate: expected})


def _cmems_slice(dataset: Any, payload: dict[str, Any]) -> Any:
    """Apply the Toolbox's daily temporal selection, including boundary neighbours.

    An `outside` selection can include samples on the next day; an instant can
    lie between two samples. Merely grouping by UTC date would lose those data.
    """
    import numpy as np

    if "time" not in dataset.dims or dataset.sizes["time"] == 0:
        raise ValueError("CMEMS batch requires a nonempty time dimension")
    index = dataset.indexes["time"]
    start = np.datetime64(payload["start_datetime"])
    end = np.datetime64(payload["end_datetime"])
    if not index.is_monotonic_increasing:
        start, end = end, start
    method = payload["coordinates_selection_method"]
    if method in {"outside", "nearest"}:
        adjusted = []
        for value, side in [(start, "pad"), (end, "backfill")]:
            if index.min() <= value <= index.max():
                position = index.get_indexer([value], method="nearest" if method == "nearest" else side)[0]
                value = index[position]
            adjusted.append(value)
        start, end = adjusted
    selected = dataset.sel(time=slice(start, end))
    if selected.sizes["time"] == 0:
        nearest = index[index.get_indexer([start], method="nearest")[0]]
        selected = dataset.sel(time=slice(nearest, nearest))
    return selected


def _prepare_daily(dataset: Any) -> Any:
    """Narrow standard coverage attributes and storage chunks to the daily slice."""
    import numpy as np

    daily = dataset.copy(deep=False)
    coordinate = "valid_time" if "valid_time" in daily.coords else "time"
    if coordinate in daily.coords:
        times = daily[coordinate].values
        start, end = np.min(times), np.max(times)
        for name, value in (("time_coverage_start", start), ("time_coverage_end", end)):
            if name in daily.attrs:
                daily.attrs[name] = str(np.datetime_as_string(value, unit="ns", timezone="UTC"))
        if "time_coverage_duration" in daily.attrs:
            seconds = float((end - start) / np.timedelta64(1, "s"))
            daily.attrs["time_coverage_duration"] = f"PT{seconds:g}S"
    for variable in daily.variables.values():
        chunks = variable.encoding.get("chunksizes")
        if chunks is not None:
            variable.encoding = dict(variable.encoding)
            if len(chunks) != variable.ndim:
                # Selecting a scalar forecast run can remove an encoded dimension.
                variable.encoding.pop("chunksizes")
            else:
                variable.encoding["chunksizes"] = tuple(
                    min(chunk, size) for chunk, size in zip(chunks, variable.shape, strict=True)
                )
    return daily
