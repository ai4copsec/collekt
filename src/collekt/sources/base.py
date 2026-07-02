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


@dataclass(frozen=True)
class SourceAdapter:
    """A registered source adapter.

    An adapter knows how to `fetch` source-native files for a request and how to
    `plan` (dry-run) the same work without touching the network or disk. Both
    return `SourceResult` records.

    Args:
        kind: The ``kind`` value that selects this adapter in source config.
        fetch: ``(request, source, config, request_dir, *, progress) -> list[SourceResult]``.
        plan: ``(request, source, config, request_dir) -> list[SourceResult]``.
    """

    kind: str
    fetch: Callable[..., list[SourceResult]]
    plan: Callable[..., list[SourceResult]]


_ADAPTERS: dict[str, SourceAdapter] = {}


def register_adapter(adapter: SourceAdapter) -> None:
    """Register a source adapter by its ``kind``."""
    _ADAPTERS[adapter.kind] = adapter


def get_adapter(kind: str) -> SourceAdapter | None:
    """Return the registered adapter for ``kind``, or `None` if unknown."""
    return _ADAPTERS.get(kind)


def registered_kinds() -> tuple[str, ...]:
    """Return the sorted names of all registered source kinds."""
    return tuple(sorted(_ADAPTERS))
