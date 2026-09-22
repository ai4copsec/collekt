"""Shared source-adapter types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collekt.core.config import Config, SourceConfig
    from collekt.core.diagnostics import DoctorCheck
    from collekt.core.request import Request


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


def should_reuse_cache(exists: bool, config: Config) -> bool:
    """Return whether an already-staged file should be reused instead of re-fetched.

    Args:
        exists: Whether the output for this day/request already exists on disk.
        config: Global configuration (cache policy).
    """
    return exists and config.cache.reuse_existing and not config.cache.overwrite


def missing_after_fetch(
    source: SourceConfig,
    dataset_id: str,
    *,
    day: str | None = None,
    details: dict[str, Any] | None = None,
    extra: str | None = None,
) -> SourceResult:
    """Build the SKIPPED result for a fetch that reported success but wrote nothing.

    Args:
        source: Source-specific configuration.
        dataset_id: Provider dataset identifier for this result.
        day: Requested day, if this is a per-day result.
        details: Provider-specific detail payload.
        extra: Optional extra context appended to the message (e.g. the
            provider's own response repr).
    """
    message = "download completed but file is missing"
    if extra:
        message = f"{message} ({extra})"
    return SourceResult(
        source=source.name,
        status=SourceStatus.SKIPPED,
        dataset_id=dataset_id,
        variables=source.variables,
        message=message,
        day=day,
        details=details,
    )


class BatchHandler(Protocol):
    """Call signature every `SourceAdapter.batch` implementation shares.

    A handler covers both planning and execution so that a dry run reports the
    same grouping the download will use. It returns the same `SourceResult`
    records `fetch` and `plan` return, at the same output paths.
    """

    def __call__(
        self,
        request: Request,
        source: SourceConfig,
        config: Config,
        request_dir: Path,
        *,
        batch_days: int,
        dry_run: bool,
        progress: ProgressCallback,
    ) -> list[SourceResult]:
        """Group up to `batch_days` UTC calendar days of retrieval work."""
        ...


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
        diagnose: Optional ``(config, online) -> list[DoctorCheck]`` hook for
            package, credential, and provider-catalogue diagnostics.
        known_raw_keys: Provider-specific keys this adapter reads from
            `SourceConfig.raw`. `collekt doctor` warns about any other key on a
            source of this kind, since a typo in one is otherwise silently
            ignored (the adapter just falls back to its default).
        batch: Optional opt-in `BatchHandler`. It must preserve output
            semantics and use the same grouping for planning and execution.
        batch_check: Optional ``(source) -> list[str]`` hook returning the
            reasons this source cannot be batched. `run_collection` calls it
            before any download so a rejected configuration never leaves a
            half-finished collection behind.
    """

    kind: str
    fetch: Callable[..., list[SourceResult]]
    plan: Callable[..., list[SourceResult]]
    diagnose: Callable[[Config, bool], list[DoctorCheck]] | None = None
    known_raw_keys: frozenset[str] = frozenset()
    batch: BatchHandler | None = None
    batch_check: Callable[[SourceConfig], list[str]] | None = None


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
