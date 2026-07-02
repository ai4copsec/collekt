"""Temporal sampling helpers for source planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from collekt.core.config import SourceConfig
from collekt.core.request import Request, format_sampling, parse_sampling


@dataclass(frozen=True)
class SamplingPlan:
    """Resolved temporal sampling for one request/source pair."""

    requested_sampling: str
    actual_sampling: str
    requested_timestamps: tuple[datetime, ...]
    source_timestamps: tuple[datetime, ...]
    warning: str | None = None

    def timestamps_for_day(self, day) -> tuple[datetime, ...]:
        """Return source timestamps that fall on a UTC day."""
        return tuple(timestamp for timestamp in self.source_timestamps if timestamp.date() == day)

    def as_dict(self) -> dict[str, object]:
        """Return JSON-serializable temporal planning metadata."""
        data: dict[str, object] = {
            "requested_sampling": self.requested_sampling,
            "actual_sampling": self.actual_sampling,
            "requested_timestamps": [_format_datetime(timestamp) for timestamp in self.requested_timestamps],
            "source_timestamps": [_format_datetime(timestamp) for timestamp in self.source_timestamps],
        }
        if self.warning is not None:
            data["warning"] = self.warning
        return data

    def day_dict(self, day) -> dict[str, object]:
        """Return JSON-serializable temporal metadata for one source/day result."""
        data = self.as_dict()
        data["source_timestamps"] = [_format_datetime(timestamp) for timestamp in self.timestamps_for_day(day)]
        return data


def sampling_plan(request: Request, source: SourceConfig) -> SamplingPlan:
    """Resolve the actual source sampling for a request.

    Sources choose the requested sampling when they declare it as supported.
    Otherwise the planner chooses the finest supported sampling that is not
    finer than the request. If no coarser option exists, it falls back to the
    coarsest supported option and records a warning.
    """
    requested_hours = request.sampling_hours
    supported_hours = sorted(parse_sampling(value) for value in source.temporal.supported_sampling)
    if requested_hours in supported_hours:
        actual_hours = requested_hours
        warning = None
    else:
        coarser = [hours for hours in supported_hours if hours > requested_hours]
        actual_hours = min(coarser) if coarser else max(supported_hours)
        warning = (
            f"{source.name} does not support {format_sampling(requested_hours)} sampling; "
            f"using {format_sampling(actual_hours)}"
        )
    return SamplingPlan(
        requested_sampling=format_sampling(requested_hours),
        actual_sampling=format_sampling(actual_hours),
        requested_timestamps=tuple(_iter_timestamps(request.start_datetime, request.end_datetime, requested_hours)),
        source_timestamps=tuple(_iter_timestamps(request.start_datetime, request.end_datetime, actual_hours)),
        warning=warning,
    )


def _iter_timestamps(start: datetime, end: datetime, sampling_hours: int):
    current = start
    step = timedelta(hours=sampling_hours)
    while current <= end:
        yield current
        current += step


def day_bounds(request: Request, day) -> tuple[datetime, datetime]:
    """Return request-clipped bounds for one UTC calendar day."""
    start = datetime.combine(day, time.min, tzinfo=request.start_datetime.tzinfo)
    end = datetime.combine(day, time.max, tzinfo=request.end_datetime.tzinfo)
    return max(start, request.start_datetime), min(end, request.end_datetime)


def _format_datetime(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
