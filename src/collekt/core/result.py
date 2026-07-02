"""Result object returned by collekt collection operations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from collekt.core.reporting import Summary
from collekt.sources.base import SourceResult


@dataclass(frozen=True)
class Result:
    """Result returned by collekt fetch operations."""

    output_dir: Path
    manifest_path: Path
    files: tuple[Path, ...]
    results: tuple[SourceResult, ...]
    summary: Summary
