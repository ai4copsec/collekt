"""Source availability checks used during collection planning.

`plan()` resolves, for every source, whether the requested region and dates are
covered before any download is attempted. Two complementary mechanisms back this:

- **Declarative coverage** — a static `coverage` block in a source's
  configuration (spatial box plus an optional date range, where dates may be
  relative such as ``now-5d``). Used for products without a cheap online
  catalogue.
- **Online catalogue (`describe`)** — CMEMS dataset extents queried through the
  Copernicus Marine Toolbox. When the toolbox or network is unavailable the
  check degrades to ``unknown`` instead of failing.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

GLOBAL_WEST, GLOBAL_EAST, GLOBAL_SOUTH, GLOBAL_NORTH = -180.0, 180.0, -90.0, 90.0

_RELATIVE = re.compile(r"now\s*(?:([+-])\s*(\d+)\s*([dw]?))?")


def today() -> date:
    """Return the current UTC date."""
    return datetime.now(UTC).date()


@dataclass(frozen=True)
class Coverage:
    """Spatial box and optional date range a source can serve.

    A `None` ``start`` or ``end`` means the corresponding bound is open.
    """

    west: float
    east: float
    south: float
    north: float
    start: date | None
    end: date | None
    kind: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping for manifests."""
        return {
            "west": self.west,
            "east": self.east,
            "south": self.south,
            "north": self.north,
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
            "kind": self.kind,
        }


@dataclass(frozen=True)
class Availability:
    """Availability verdict for one source/day during planning."""

    status: str  # "available" | "unavailable" | "unknown" | "not_checked"
    method: str  # "describe" | "coverage" | "not_checked"
    reason: str | None = None
    coverage: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping for manifests."""
        return {"status": self.status, "method": self.method, "reason": self.reason, "coverage": self.coverage}


def parse_relative_date(value: Any, *, default: date | None = None) -> date | None:
    """Parse an ISO date or a ``now``-relative expression.

    Example:
        ```python
        parse_relative_date("2023-04-01")  # -> date(2023, 4, 1)
        parse_relative_date("now-5d")  # -> five days before today
        parse_relative_date(None)  # -> default
        ```

    Args:
        value: ISO date string, `datetime.date`, ``now``, ``now-<N>d``,
            ``now+<N>w``, or `None`.
        default: Value returned when ``value`` is `None` or empty.

    Returns:
        A `datetime.date`, or ``default`` when no value is given.
    """
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    match = _RELATIVE.fullmatch(text)
    if match:
        if match.group(1) is None:
            return today()
        sign = 1 if match.group(1) == "+" else -1
        amount = int(match.group(2))
        days = amount * (7 if match.group(3) == "w" else 1)
        return today() + timedelta(days=sign * days)
    return date.fromisoformat(text[:10])


def parse_coverage(raw: Mapping[str, Any]) -> Coverage:
    """Parse a configured ``coverage`` block into a `Coverage`.

    Spatial bounds default to the whole globe; date bounds default to open. The
    preferred shape is:

    ```yaml
    coverage:
      longitude: [west, east]
      latitude: [south, north]
      temporal:
        start: "2020-01-01"
        end: null
        kind: rolling
    ```

    The flat legacy keys (`west`, `east`, `south`, `north`, `start`, `end`) are
    still accepted.
    """
    longitude = _range(raw.get("longitude") or raw.get("lon"), raw.get("west"), raw.get("east"))
    latitude = _range(raw.get("latitude") or raw.get("lat"), raw.get("south"), raw.get("north"))
    temporal = raw.get("temporal") or {}
    if not isinstance(temporal, Mapping):
        raise ValueError("coverage.temporal must be a mapping")
    return Coverage(
        west=float(longitude[0] if longitude else GLOBAL_WEST),
        east=float(longitude[1] if longitude else GLOBAL_EAST),
        south=float(latitude[0] if latitude else GLOBAL_SOUTH),
        north=float(latitude[1] if latitude else GLOBAL_NORTH),
        start=parse_relative_date(temporal.get("start", raw.get("start"))),
        end=parse_relative_date(temporal.get("end", raw.get("end"))),
        kind=_optional_text(temporal.get("kind", raw.get("kind"))),
    )


def _range(value: Any, start: Any, end: Any) -> tuple[Any, Any] | None:
    if value is None:
        if start is None and end is None:
            return None
        return start, end
    if isinstance(value, Mapping):
        return value.get("west", value.get("south", value.get("min"))), value.get(
            "east", value.get("north", value.get("max"))
        )
    items = tuple(value)
    if len(items) != 2:
        raise ValueError("coverage ranges must contain exactly two values")
    return items


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def static_coverage(source: Any) -> Coverage | None:
    """Return a source's declarative `coverage` block, if it has one."""
    block = (getattr(source, "raw", None) or {}).get("coverage")
    return parse_coverage(block) if block else None


def region_overlaps(region: Any, coverage: Coverage) -> bool:
    """Return whether the request region intersects the coverage box."""
    return not (
        region.east < coverage.west
        or region.west > coverage.east
        or region.north < coverage.south
        or region.south > coverage.north
    )


def day_in_range(day: date, coverage: Coverage) -> bool:
    """Return whether a UTC day falls within the coverage date range."""
    return (coverage.start is None or day >= coverage.start) and (coverage.end is None or day <= coverage.end)


def clip_region(region: Any, coverage: Coverage) -> tuple[float, float, float, float]:
    """Return the request bounds clipped to the coverage box."""
    return (
        max(region.west, coverage.west),
        min(region.east, coverage.east),
        max(region.south, coverage.south),
        min(region.north, coverage.north),
    )


def region_reason(region: Any, coverage: Coverage) -> str:
    """Return a message explaining an out-of-region skip."""
    return (
        f"region (west={region.west}, east={region.east}, south={region.south}, north={region.north}) "
        f"is outside the available coverage (west={coverage.west}, east={coverage.east}, "
        f"south={coverage.south}, north={coverage.north})"
    )


def day_reason(day: date, coverage: Coverage) -> str:
    """Return a message explaining an out-of-range skip."""
    start = coverage.start.isoformat() if coverage.start else "-inf"
    end = coverage.end.isoformat() if coverage.end else "+inf"
    return f"{day.isoformat()} is outside the available date range ({start} to {end})"


def _copernicusmarine_describe():
    try:
        import copernicusmarine
    except ImportError:
        return None
    return getattr(copernicusmarine, "describe", None)


def describe_coverage(dataset_id: str, *, describe=None) -> Coverage | None:
    """Return a CMEMS dataset's coverage from the Copernicus Marine catalogue.

    Args:
        dataset_id: CMEMS dataset identifier.
        describe: Optional ``copernicusmarine.describe`` callable, mainly for
            testing. When omitted it is resolved from the installed toolbox.

    Returns:
        The dataset `Coverage`, or `None` when the toolbox is unavailable, the
        catalogue lookup fails, or the dataset is not found.
    """
    describe = describe if describe is not None else _copernicusmarine_describe()
    if describe is None:
        return None
    try:
        catalogue = describe(dataset_id=dataset_id, disable_progress_bar=True, raise_on_error=True)
    except Exception:  # noqa: BLE001 - provider/network errors degrade to "unknown", never crash planning
        return None
    return _coverage_from_catalogue(catalogue, dataset_id)


def merge_coverages(coverages: list[Coverage]) -> Coverage:
    """Combine several coverages into their bounding envelope."""
    starts = [c.start for c in coverages]
    ends = [c.end for c in coverages]
    return Coverage(
        west=min(c.west for c in coverages),
        east=max(c.east for c in coverages),
        south=min(c.south for c in coverages),
        north=max(c.north for c in coverages),
        start=None if any(s is None for s in starts) else min(s for s in starts if s is not None),
        end=None if any(e is None for e in ends) else max(e for e in ends if e is not None),
    )


def _coverage_from_catalogue(catalogue: Any, dataset_id: str) -> Coverage | None:
    wests: list[float] = []
    easts: list[float] = []
    souths: list[float] = []
    norths: list[float] = []
    starts: list[date] = []
    ends: list[date] = []
    found = False
    for product in getattr(catalogue, "products", []) or []:
        for dataset in getattr(product, "datasets", []) or []:
            if getattr(dataset, "dataset_id", None) != dataset_id:
                continue
            found = True
            for version in getattr(dataset, "versions", []) or []:
                for part in getattr(version, "parts", []) or []:
                    for service in getattr(part, "services", []) or []:
                        for variable in getattr(service, "variables", []) or []:
                            _accumulate_variable(variable, wests, easts, souths, norths, starts, ends)
    if not found:
        return None
    return Coverage(
        west=min(wests) if wests else GLOBAL_WEST,
        east=max(easts) if easts else GLOBAL_EAST,
        south=min(souths) if souths else GLOBAL_SOUTH,
        north=max(norths) if norths else GLOBAL_NORTH,
        start=min(starts) if starts else None,
        end=max(ends) if ends else None,
    )


def _accumulate_variable(variable, wests, easts, souths, norths, starts, ends) -> None:
    bbox = getattr(variable, "bbox", None)
    if bbox and len(bbox) == 4:
        west, south, east, north = bbox  # copernicusmarine bbox order: [min_lon, min_lat, max_lon, max_lat]
        wests.append(float(west))
        souths.append(float(south))
        easts.append(float(east))
        norths.append(float(north))
    for coord in getattr(variable, "coordinates", []) or []:
        coordinate_id = (getattr(coord, "coordinate_id", "") or "").lower()
        unit = getattr(coord, "coordinate_unit", None)
        if coordinate_id == "time":
            low = _coordinate_to_date(getattr(coord, "minimum_value", None), unit)
            high = _coordinate_to_date(getattr(coord, "maximum_value", None), unit)
            if low is not None:
                starts.append(low)
            if high is not None:
                ends.append(high)
        elif coordinate_id in ("longitude", "lon") and not bbox:
            _append_minmax(coord, wests, easts)
        elif coordinate_id in ("latitude", "lat") and not bbox:
            _append_minmax(coord, souths, norths)


def _append_minmax(coord, lows: list[float], highs: list[float]) -> None:
    low = getattr(coord, "minimum_value", None)
    high = getattr(coord, "maximum_value", None)
    if isinstance(low, (int, float)):
        lows.append(float(low))
    if isinstance(high, (int, float)):
        highs.append(float(high))


def _coordinate_to_date(value: Any, unit: str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    text = (unit or "").lower()
    if "since" in text:
        scale, reference = text.split("since", 1)
    else:
        scale, reference = text, ""
    seconds_per_unit = {
        "millisecond": 1e-3,
        "milliseconds": 1e-3,
        "second": 1.0,
        "seconds": 1.0,
        "minute": 60.0,
        "minutes": 60.0,
        "hour": 3600.0,
        "hours": 3600.0,
        "day": 86400.0,
        "days": 86400.0,
    }
    factor = next((sec for word, sec in seconds_per_unit.items() if word in scale), 1e-3)
    try:
        epoch = datetime.fromisoformat(reference.strip()[:19]) if reference.strip() else datetime(1970, 1, 1)
    except ValueError:
        epoch = datetime(1970, 1, 1)
    try:
        return (epoch + timedelta(seconds=float(value) * factor)).date()
    except (ValueError, OverflowError):
        return None
