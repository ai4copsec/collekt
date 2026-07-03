"""Temporal sampling helpers for source planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from collekt.core.config import SourceConfig
from collekt.core.request import Request, format_sampling_minutes, parse_sampling


@dataclass(frozen=True)
class SamplingPlan:
    """Resolved temporal sampling for one request/source pair."""

    requested_sampling: str
    actual_sampling: str
    requested_timestamps: tuple[datetime, ...]
    source_timestamps: tuple[datetime, ...]

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
        return data

    def day_dict(self, day) -> dict[str, object]:
        """Return JSON-serializable temporal metadata for one source/day result."""
        data = self.as_dict()
        data["source_timestamps"] = [_format_datetime(timestamp) for timestamp in self.timestamps_for_day(day)]
        return data


def sampling_plan(request: Request, source: SourceConfig) -> SamplingPlan:
    """Resolve the download sampling for a request/source pair.

    A source declares the dataset's actual sampling. A request may download at
    that cadence or any coarser cadence that is an integer multiple of it.
    """
    requested_minutes = request.sampling_minutes
    dataset_minutes = parse_sampling(source.temporal_sampling)
    if requested_minutes < dataset_minutes or requested_minutes % dataset_minutes:
        raise ValueError(
            f"{source.name} has {format_sampling_minutes(dataset_minutes)} dataset sampling; "
            f"requested sampling {format_sampling_minutes(requested_minutes)} must be an integer multiple of it"
        )
    return SamplingPlan(
        requested_sampling=format_sampling_minutes(requested_minutes),
        actual_sampling=format_sampling_minutes(requested_minutes),
        requested_timestamps=tuple(_iter_timestamps(request.start_datetime, request.end_datetime, requested_minutes)),
        source_timestamps=tuple(_iter_timestamps(request.start_datetime, request.end_datetime, requested_minutes)),
    )


def _iter_timestamps(start: datetime, end: datetime, sampling_minutes: int):
    current = start
    step = timedelta(minutes=sampling_minutes)
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
