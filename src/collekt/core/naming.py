"""Output naming helpers."""

from __future__ import annotations

import hashlib
import string
from datetime import date, datetime
from pathlib import Path
from typing import Any

from collekt.core.request import Region


class PatternError(ValueError):
    """Raised when an output filename pattern cannot be formatted."""


def bbox_hash(region: Region) -> str:
    """Return a stable short hash for a region."""
    text = f"{region.west:.6f},{region.east:.6f},{region.south:.6f},{region.north:.6f}"
    return hashlib.sha1(text.encode("ascii")).hexdigest()[:8]


def format_pattern(pattern: str, values: dict[str, Any]) -> str:
    """Format a configured output pattern with early validation."""
    names = {field_name for _, field_name, _, _ in string.Formatter().parse(pattern) if field_name}
    missing = sorted(name.split(".", maxsplit=1)[0].split("[", maxsplit=1)[0] for name in names if name not in values)
    if missing:
        raise PatternError(f"unknown placeholder(s) in pattern {pattern!r}: {', '.join(missing)}")
    try:
        return pattern.format(**values)
    except Exception as exc:  # noqa: BLE001 - convert formatting details to config error
        raise PatternError(f"could not format pattern {pattern!r}: {exc}") from exc


def pattern_values(
    *,
    source: str,
    dataset_id: str | None,
    region: Region,
    start: datetime,
    end: datetime,
    day: date | None = None,
    sampling: str = "24h",
) -> dict[str, Any]:
    """Return the standard placeholder mapping for output patterns."""
    return {
        "source": source,
        "dataset_id": dataset_id or source,
        "date": day or start.date(),
        "start": start,
        "end": end,
        "sampling": sampling,
        "bbox_hash": bbox_hash(region),
        "west": region.west,
        "east": region.east,
        "south": region.south,
        "north": region.north,
    }


def request_directory(root: Path, pattern: str, values: dict[str, Any]) -> Path:
    """Return the output directory for one collection request."""
    return root / format_pattern(pattern, values)
