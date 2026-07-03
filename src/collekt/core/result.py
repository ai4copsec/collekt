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

    def __str__(self) -> str:
        """Return a compact human-readable rendering."""
        lines = [str(self.summary), f"manifest: {self.manifest_path}"]
        for item in self.results:
            target = item.path if item.path is not None else item.message
            lines.append(f"  {item.source}: {item.status.value} -> {target}")
        return "\n".join(lines)

    __repr__ = __str__

    def render(self) -> None:
        """Print the result summary and per-source statuses.

        Example:
            ```python
            result = fetcher.plan()
            result.render()
            ```
        """
        print(self)
