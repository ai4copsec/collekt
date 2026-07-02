"""Shared source-adapter types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class SourceStatus(StrEnum):
    """Collection status for one source/day."""

    PLANNED = "planned"
    DOWNLOADED = "downloaded"
    REUSED = "reused"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceResult:
    """Result from one source adapter."""

    source: str
    status: SourceStatus
    path: Path | None = None
    dataset_id: str | None = None
    variables: tuple[str, ...] = ()
    message: str | None = None
    day: str | None = None
    format: str = "netcdf"
    details: dict[str, Any] | None = None
    inspection: dict[str, Any] | None = None

    @property
    def is_warning(self) -> bool:
        """Return whether this result should be displayed as a warning."""
        return self.status in {SourceStatus.SKIPPED, SourceStatus.FAILED}


ProgressCallback = Callable[[str, str], None]


def null_progress(_source: str, _message: str) -> None:
    """Progress callback that ignores all events."""
