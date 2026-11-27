"""ECMWF Open Data: the freshest IFS value at every time step.

Each time on the source's grid (3-hourly) is read from the newest IFS run that
covers it. Once its own 6-hourly run is published, a time is read from that run
(lead 0 h, the analysis, or +3 h). Times after the newest published run are read
from that run's forecast. When a newer run is published, the times read from an
older forecast are read again from the newer run: values always come from the
freshest run available when the request is made.

Caching works in two layers:

- **pieces**: one cropped NetCDF per run, lead time and region, kept under
  ``<output.root>/<path>/pieces/`` and shared by every request. A run's lead
  time never changes, so a rolling window (e.g. the last 72 hours) downloads
  only the pieces earlier requests did not.
- **day files**: one NetCDF per request day in the request directory, built from
  pieces and named after the newest run it uses (``{run}``) and the hours it
  spans (``{hours}``, e.g. ``00-21``, or ``00-06_12-21`` around a gap). A newer
  run or a filled gap changes the name, so the day is rebuilt.

A lead time that fails to download leaves a gap at its time step instead of
dropping the day; the next request retries it.

Each day file has a ``valid_time`` dimension with two coordinates along it:
``source_cycle`` (the run each value was read from) and ``forecast_step`` (the
lead time from that run, in hours).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from collekt.core.availability import (
    Availability,
    AvailabilityMethod,
    AvailabilityStatus,
    day_in_range,
    day_reason,
    static_coverage,
)
from collekt.core.config import Config, SourceConfig
from collekt.core.naming import bbox_hash
from collekt.core.request import Request, format_sampling_minutes, parse_sampling
from collekt.core.temporal import SamplingPlan
from collekt.sources.base import (
    ProgressCallback,
    SourceAdapter,
    SourceResult,
    SourceStatus,
    null_progress,
    register_adapter,
    should_reuse_cache,
)
from collekt.sources.ecmwf_open_data import (
    DEFAULT_DATASET_ID,
    DEFAULT_PAD_DEG,
    _client_class,
    _crop_to_netcdf,
    _grid_resolution,
    _padded_bbox,
    _published_steps,
)
from collekt.sources.planning import output_path

RUN_INTERVAL = timedelta(hours=6)
# Runs at least this old are always published, so times read from them need no probe.
PUBLICATION_MARGIN = timedelta(days=1)
# Every run publishes this lead time, and all its lead times appear together.
PROBE_STEP = 144
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class _Slot:
    """One time step and the run/lead time it is read from (``None`` if unavailable)."""

    time: datetime
    run: datetime | None = None
    step: int | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        if self.run is None:
            return {"time": _iso(self.time), "unavailable": self.reason}
        return {"time": _iso(self.time), "run": _iso(self.run), "step": self.step}


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _floor(value: datetime, interval: timedelta) -> datetime:
    return value - (value - _EPOCH) % interval


def _slot_times(request: Request, sampling: timedelta) -> list[datetime]:
    """Return the times on the source's grid that lie within the request window."""
    start = request.start_datetime
    current = _floor(start, sampling)
    if current < start:
        current += sampling
    times = []
    while current <= request.end_datetime:
        times.append(current)
        current += sampling
    return times


def _newest_published_run(source: SourceConfig) -> datetime:
    """Return the newest run published on ECMWF Open Data (``HEAD`` probes; raises if none)."""
    client = _client_class()(
        source=str(source.raw.get("source", "ecmwf")),
        model=str(source.raw.get("model", "ifs")),
        resol=str(source.raw.get("resol", "0p25")),
    )
    latest = client.latest(
        type=str(source.raw.get("type", "fc")),
        stream=str(source.raw.get("stream", "oper")),
        levtype=str(source.raw.get("levtype", "sfc")),
        step=PROBE_STEP,
    )
    return latest.replace(tzinfo=UTC)


def _newest_runs(source: SourceConfig, times: list[datetime]) -> tuple[datetime, str | None]:
    """Return the newest run that ``times`` may be read from, and why it may be stale.

    Only probes ECMWF Open Data when the times reach runs that may not be published
    yet. If the probe fails, returns the newest run that is certainly published and
    the failure, so that later times are reported as unavailable.
    """
    newest_needed = _floor(times[-1], RUN_INTERVAL)
    published = _floor(_now() - PUBLICATION_MARGIN, RUN_INTERVAL)
    if newest_needed <= published:
        return newest_needed, None
    try:
        return _newest_published_run(source), None
    except Exception as exc:  # noqa: BLE001 - an unreachable provider makes recent times unavailable
        return published, f"could not find the newest published IFS run: {exc}"


def _resolve_slot(time: datetime, latest: datetime, probe_error: str | None) -> _Slot:
    """Read ``time`` from its own run once published, else from the newest run's forecast."""
    own_run = _floor(time, RUN_INTERVAL)
    if probe_error is not None and own_run > latest:
        return _Slot(time, reason=probe_error)
    run = min(own_run, latest)
    step = int((time - run).total_seconds() // 3600)
    if step not in _published_steps(run.hour):
        return _Slot(time, reason=f"no lead time of the {_iso(run)} run reaches it")
    return _Slot(time, run=run, step=step)


def _resolve(request: Request, source: SourceConfig) -> tuple[SamplingPlan, dict[date, list[_Slot]]]:
    """Resolve every time step of the request, grouped by UTC day."""
    minutes = parse_sampling(source.temporal_sampling)
    times = _slot_times(request, timedelta(minutes=minutes))
    label = format_sampling_minutes(minutes)
    plan = SamplingPlan(label, label, tuple(times), tuple(times))
    by_day: dict[date, list[_Slot]] = {day: [] for day in request.iter_days()}
    if not times:
        return plan, by_day
    latest, probe_error = _newest_runs(source, times)
    for time in times:
        by_day[time.date()].append(_resolve_slot(time, latest, probe_error))
    return plan, by_day


def _hours_label(slots: list[_Slot], sampling: timedelta) -> str:
    """Return the hours ``slots`` span, e.g. ``00-21``, or ``00-06_12-21`` around a gap."""
    ranges = [[slots[0].time, slots[0].time]]
    for slot in slots[1:]:
        if slot.time - ranges[-1][1] == sampling:
            ranges[-1][1] = slot.time
        else:
            ranges.append([slot.time, slot.time])
    return "_".join(f"{first:%H}" if first == last else f"{first:%H}-{last:%H}" for first, last in ranges)


def _day_path(request: Request, source: SourceConfig, request_dir: Path, day: date, slots: list[_Slot]) -> Path:
    """Return the day file path, named after the newest run and the hours of the available ``slots``."""
    newest = max(slot.run for slot in slots)
    return output_path(
        request,
        source,
        request_dir,
        source.dataset_id or DEFAULT_DATASET_ID,
        day,
        run=f"{newest:%Y%m%d%H}",
        hours=_hours_label(slots, timedelta(minutes=parse_sampling(source.temporal_sampling))),
    )


def _piece_path(request: Request, source: SourceConfig, config: Config, slot: _Slot) -> Path:
    """Return the shared cache path of one run's lead time over the request region."""
    name = f"{slot.run:%Y%m%d%H}_{slot.step:03d}h_{bbox_hash(request.region)}.nc"
    return Path(config.output.root) / source.path / "pieces" / name


def _unavailable_message(slots: list[_Slot]) -> str | None:
    """Return which time steps have no value and why, grouped by reason, or ``None``."""
    by_reason: dict[str, list[str]] = {}
    for slot in slots:
        if slot.run is None:
            by_reason.setdefault(slot.reason or "unavailable", []).append(f"{slot.time:%H}")
    if not by_reason:
        return None
    return "; ".join(f"no value at {', '.join(hours)} UTC: {reason}" for reason, hours in by_reason.items())


def _day_result(
    source: SourceConfig,
    day: date,
    plan: SamplingPlan,
    slots: list[_Slot],
    status: SourceStatus,
    *,
    path: Path | None = None,
    message: str | None = None,
    details: dict[str, Any] | None = None,
) -> SourceResult:
    return SourceResult(
        source=source.name,
        status=status,
        path=path,
        dataset_id=source.dataset_id or DEFAULT_DATASET_ID,
        variables=source.variables,
        message=message,
        day=day.isoformat(),
        format="netcdf",
        details={"temporal": plan.day_dict(day), "slots": [slot.as_dict() for slot in slots]} | (details or {}),
    )


def _skip_reason(source: SourceConfig, day: date, slots: list[_Slot]) -> str | None:
    """Return why a day has nothing to read, or ``None`` if some time step is available."""
    coverage = static_coverage(source)
    if coverage is not None and not day_in_range(day, coverage):
        return day_reason(day, coverage)
    if not slots:
        return "no time step of the request falls on this day"
    if all(slot.run is None for slot in slots):
        return _unavailable_message(slots)
    return None


def plan_ecmwf_open_data_latest(
    request: Request, source: SourceConfig, config: Config, request_dir: Path
) -> list[SourceResult]:
    """Plan which run and lead time each time step is read from.

    Probes ECMWF Open Data for the newest run when the request reaches the last day.
    """
    plan, by_day = _resolve(request, source)
    coverage = static_coverage(source)
    results = []
    for day, slots in by_day.items():
        if (reason := _skip_reason(source, day, slots)) is not None:
            results.append(_day_result(source, day, plan, slots, SourceStatus.SKIPPED, message=reason))
            continue
        available = [slot for slot in slots if slot.run is not None]
        south, north, west, east = _padded_bbox(
            request.region,
            float(source.raw.get("pad_deg", DEFAULT_PAD_DEG)),
            _grid_resolution(str(source.raw.get("resol", "0p25"))),
        )
        availability = Availability(
            AvailabilityStatus.AVAILABLE,
            AvailabilityMethod.COVERAGE,
            coverage=coverage.as_dict() if coverage else None,
        )
        details = {
            "provider": "ecmwf-opendata",
            "method": "retrieve",
            "crop": {"south": south, "north": north, "west": west, "east": east},
            "availability": availability.as_dict(),
        }
        results.append(
            _day_result(
                source,
                day,
                plan,
                slots,
                SourceStatus.PLANNED,
                path=_day_path(request, source, request_dir, day, available),
                message=_unavailable_message(slots),
                details=details,
            )
        )
    return results


def _retrieve_piece(client: Any, request: Request, source: SourceConfig, slot: _Slot, path: Path) -> list[str]:
    """Download one run's lead time, crop it to the request region, and write NetCDF.

    The piece is written under a temporary name and renamed once complete, so an
    interrupted write never leaves a partial piece that later requests would reuse.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    raw_path = path.with_suffix(".raw.grib2")
    partial_path = path.with_suffix(".partial.nc")
    body = {
        "date": f"{slot.run:%Y%m%d}",
        "time": slot.run.hour,
        "step": slot.step,
        "type": str(source.raw.get("type", "fc")),
        "levtype": str(source.raw.get("levtype", "sfc")),
        "stream": str(source.raw.get("stream", "oper")),
        "param": list(source.variables),
    }
    try:
        client.retrieve(body, target=str(raw_path))
        resol = str(source.raw.get("resol", "0p25"))
        missing = _crop_to_netcdf(
            raw_path,
            partial_path,
            request.region,
            float(source.raw.get("pad_deg", DEFAULT_PAD_DEG)),
            _grid_resolution(resol),
            source.variables,
        )
        partial_path.replace(path)
        return missing
    finally:
        raw_path.unlink(missing_ok=True)
        partial_path.unlink(missing_ok=True)


def _write_day(pieces: list[tuple[_Slot, Path]], path: Path) -> None:
    """Stack a day's pieces along ``valid_time``, recording each value's run and lead time."""
    import numpy as np
    import xarray as xr

    parts = []
    for slot, piece in pieces:
        with xr.open_dataset(piece) as dataset:
            loaded = dataset.load()
        loaded = loaded.drop_vars([name for name in ("time", "step", "valid_time") if name in loaded.coords])
        parts.append(
            loaded.expand_dims(valid_time=[np.datetime64(slot.time.replace(tzinfo=None), "ns")]).assign_coords(
                source_cycle=("valid_time", [np.datetime64(slot.run.replace(tzinfo=None), "ns")]),
                forecast_step=("valid_time", [slot.step]),
            )
        )
    day = xr.concat(parts, dim="valid_time", coords="minimal", compat="override")
    day["source_cycle"].attrs["long_name"] = "IFS run the value was read from"
    day["forecast_step"].attrs.update(long_name="lead time from source_cycle", units="hours")
    day.to_netcdf(path)


def fetch_ecmwf_open_data_latest(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    *,
    progress: ProgressCallback = null_progress,
) -> list[SourceResult]:
    """Fetch the freshest IFS value at every time step, cropped to the request region.

    A lead time that fails to download is reported as unavailable; the day file is
    written from the other time steps, under a name that shows the gap.
    """
    plan, by_day = _resolve(request, source)
    client = None
    results = []
    for day, slots in by_day.items():
        if (reason := _skip_reason(source, day, slots)) is not None:
            results.append(_day_result(source, day, plan, slots, SourceStatus.SKIPPED, message=reason))
            continue
        available = [slot for slot in slots if slot.run is not None]
        path = _day_path(request, source, request_dir, day, available)
        message = _unavailable_message(slots)
        if should_reuse_cache(path.exists(), config):
            results.append(_day_result(source, day, plan, slots, SourceStatus.REUSED, path=path, message=message))
            continue
        pieces: list[tuple[_Slot, Path]] = []
        failed: dict[datetime, str] = {}
        missing_params: set[str] = set()
        for slot in available:
            piece = _piece_path(request, source, config, slot)
            if not should_reuse_cache(piece.exists(), config):
                progress(source.name, f"downloading +{slot.step} h of the {_iso(slot.run)} IFS run")
                try:
                    if client is None:
                        client = _client_class()(
                            source=str(source.raw.get("source", "ecmwf")),
                            model=str(source.raw.get("model", "ifs")),
                            resol=str(source.raw.get("resol", "0p25")),
                        )
                    missing_params.update(_retrieve_piece(client, request, source, slot, piece))
                except Exception as exc:  # noqa: BLE001 - provider availability failures are warnings
                    failed[slot.time] = f"download failed: {exc}"
                    continue
            pieces.append((slot, piece))
        if failed:
            slots = [_Slot(slot.time, reason=failed[slot.time]) if slot.time in failed else slot for slot in slots]
            message = _unavailable_message(slots)
        if not pieces:
            results.append(_day_result(source, day, plan, slots, SourceStatus.SKIPPED, message=message))
            continue
        if failed:
            path = _day_path(request, source, request_dir, day, [slot for slot, _ in pieces])
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            _write_day(pieces, path)
        except Exception as exc:  # noqa: BLE001 - a corrupt piece is reported, not raised
            results.append(_day_result(source, day, plan, slots, SourceStatus.SKIPPED, message=str(exc)))
            continue
        if missing_params:
            limitation = f"missing from the cropped file (provider limitation): {', '.join(sorted(missing_params))}"
            message = f"{message}; {limitation}" if message else limitation
        results.append(_day_result(source, day, plan, slots, SourceStatus.DOWNLOADED, path=path, message=message))
    return results


register_adapter(
    SourceAdapter(
        kind="ecmwf_open_data_latest",
        fetch=fetch_ecmwf_open_data_latest,
        plan=plan_ecmwf_open_data_latest,
        # No diagnose hook: the `ecmwf_open_data` adapter already checks the same packages.
        known_raw_keys=frozenset({"model", "resol", "pad_deg", "source", "type", "levtype", "stream"}),
    )
)
