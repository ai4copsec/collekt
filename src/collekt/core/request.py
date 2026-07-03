"""Request objects for collekt collection."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

VALID_SAMPLING_MINUTES = (15, 60, 180, 360, 1440)


@dataclass(frozen=True)
class Region:
    """Geographic region of interest.

    A region is a bounding box. It may also carry the original geometry as WKT
    (`geometry`) for sources that support polygon filters rather than a plain
    bounding box.
    """

    west: float
    east: float
    south: float
    north: float
    geometry: str | None = None

    @classmethod
    def from_bbox(cls, bbox: Iterable[float]) -> Region:
        """Build a region from ``(west, east, south, north)``."""
        west, east, south, north = bbox
        return cls(west=float(west), east=float(east), south=float(south), north=float(north))

    @classmethod
    def from_description_tuple(cls, roi: Iterable[float]) -> Region:
        """Build a region from ``(min_lat, max_lat, min_lon, max_lon)``."""
        min_lat, max_lat, min_lon, max_lon = roi
        return cls(west=float(min_lon), east=float(max_lon), south=float(min_lat), north=float(max_lat))

    @classmethod
    def from_point_radius(cls, latitude: float, longitude: float, radius_km: float) -> Region:
        """Build a region from a centre point and a radius in kilometres."""
        from collekt.core.geo import get_coordinates_min_max

        bounds = get_coordinates_min_max(latitude=latitude, longitude=longitude, radius_in_km=radius_km)
        return cls(
            west=float(bounds["lon_min"]),
            east=float(bounds["lon_max"]),
            south=float(bounds["lat_min"]),
            north=float(bounds["lat_max"]),
        )

    @classmethod
    def from_geojson(cls, path: str | Path) -> Region:
        """Build a region from a GeoJSON file.

        The bounding box is derived from the geometry, and the geometry itself is
        retained as WKT so polygon-capable sources can filter on it.
        """
        import json

        from collekt.core.geo import unified_geometry

        with open(path, encoding="utf-8") as handle:
            geojson = json.load(handle)
        geometry = unified_geometry(geojson)
        west, south, east, north = geometry.bounds
        return cls(west=float(west), east=float(east), south=float(south), north=float(north), geometry=geometry.wkt)

    def as_dict(self) -> dict[str, Any]:
        """Return the region as a JSON-serializable mapping."""
        data: dict[str, Any] = {"west": self.west, "east": self.east, "south": self.south, "north": self.north}
        if self.geometry is not None:
            data["geometry"] = self.geometry
        return data


def parse_datetime(value: str | date | datetime | None, *, end_of_day: bool = False) -> datetime:
    """Parse a date-like value as a timezone-aware UTC datetime."""
    if value is None:
        today = datetime.now(UTC).date()
        return datetime.combine(today, time.max if end_of_day else time.min, tzinfo=UTC)
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.max if end_of_day else time.min)
    else:
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        if "T" not in text and " " not in text:
            dt = datetime.combine(date.fromisoformat(text), time.max if end_of_day else time.min)
        else:
            dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_sampling(value: str | int | None) -> int:
    """Parse a request sampling value as one of the valid minute intervals."""
    if value is None:
        return 1440
    if isinstance(value, int):
        minutes = value * 60
    else:
        text = str(value).strip().lower()
        multiplier = 60
        for suffix in ("minutes", "minute", "mins", "min"):
            if text.endswith(suffix):
                text = text.removesuffix(suffix).strip()
                multiplier = 1
                break
        else:
            if text.endswith("m"):
                text = text.removesuffix("m").strip()
                multiplier = 1
            elif text.endswith("hours"):
                text = text.removesuffix("hours").strip()
            elif text.endswith("hour"):
                text = text.removesuffix("hour").strip()
            elif text.endswith("hrs"):
                text = text.removesuffix("hrs").strip()
            elif text.endswith("hr"):
                text = text.removesuffix("hr").strip()
            elif text.endswith("h"):
                text = text.removesuffix("h").strip()
        try:
            minutes = int(text) * multiplier
        except ValueError as exc:
            valid = ", ".join(format_sampling_minutes(minutes) for minutes in VALID_SAMPLING_MINUTES)
            raise ValueError(f"unsupported sampling {value!r}; expected one of: {valid}") from exc
    if minutes not in VALID_SAMPLING_MINUTES:
        valid = ", ".join(format_sampling_minutes(minutes) for minutes in VALID_SAMPLING_MINUTES)
        raise ValueError(f"unsupported sampling {value!r}; expected one of: {valid}")
    return minutes


def format_sampling_minutes(minutes: int) -> str:
    """Return a canonical sampling label for an already-parsed minute interval."""
    if minutes % 60:
        return f"{minutes}min"
    return f"{minutes // 60}h"


def format_sampling(value: str | int | None) -> str:
    """Return a canonical sampling label such as ``15min`` or ``6h``."""
    return format_sampling_minutes(parse_sampling(value))


@dataclass(frozen=True)
class Request:
    """A request to collect data.

    Args:
        region: Geographic region of interest.
        start: Start date or datetime. If omitted, the current UTC day is used.
        end: End date or datetime. If omitted, the start day is used.
        sampling: Requested temporal sampling interval. Supported values are
            `15min`, `1h`, `3h`, `6h`, and `24h`.
        metadata: Optional caller metadata copied to the manifest.
    """

    region: Region
    start: str | date | datetime | None = None
    end: str | date | datetime | None = None
    sampling: str | int | None = "24h"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def start_datetime(self) -> datetime:
        """Start time normalized to UTC."""
        return parse_datetime(self.start)

    @property
    def end_datetime(self) -> datetime:
        """End time normalized to UTC."""
        return parse_datetime(self.end if self.end is not None else self.start, end_of_day=True)

    @property
    def sampling_hours(self) -> int:
        """Requested sampling interval in hours."""
        minutes = self.sampling_minutes
        if minutes % 60:
            raise ValueError(f"sampling {self.sampling_label!r} is not a whole number of hours")
        return minutes // 60

    @property
    def sampling_minutes(self) -> int:
        """Requested sampling interval in minutes."""
        return parse_sampling(self.sampling)

    @property
    def sampling_label(self) -> str:
        """Requested sampling interval as a canonical label."""
        return format_sampling(self.sampling)

    def iter_days(self) -> list[date]:
        """Return UTC calendar days touched by this request."""
        start = self.start_datetime.date()
        end = self.end_datetime.date()
        days = []
        current = start
        while current <= end:
            days.append(current)
            current += timedelta(days=1)
        return days

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable request mapping."""
        return {
            "region": self.region.as_dict(),
            "start": self.start_datetime.isoformat().replace("+00:00", "Z"),
            "end": self.end_datetime.isoformat().replace("+00:00", "Z"),
            "sampling": self.sampling_label,
            "metadata": dict(self.metadata),
        }
