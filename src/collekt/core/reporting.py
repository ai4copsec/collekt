"""CLI reporting helpers."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from collekt.sources.base import SourceResult, SourceStatus


@dataclass(frozen=True)
class Summary:
    """Collection result counts."""

    planned: int = 0
    downloaded: int = 0
    reused: int = 0
    skipped: int = 0
    failed: int = 0

    @property
    def has_warnings(self) -> bool:
        """Return whether skipped or failed results occurred."""
        return self.skipped > 0 or self.failed > 0

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-serializable mapping."""
        return {
            "planned": self.planned,
            "downloaded": self.downloaded,
            "reused": self.reused,
            "skipped": self.skipped,
            "failed": self.failed,
        }


def summarize(results: Iterable[SourceResult]) -> Summary:
    """Count source results by status."""
    counts = Counter(result.status for result in results)
    return Summary(
        planned=counts[SourceStatus.PLANNED],
        downloaded=counts[SourceStatus.DOWNLOADED],
        reused=counts[SourceStatus.REUSED],
        skipped=counts[SourceStatus.SKIPPED],
        failed=counts[SourceStatus.FAILED],
    )


class CliReporter:
    """Render progress, warnings, and a summary banner."""

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    def progress(self, source: str, message: str) -> None:
        """Show a short progress spinner for one source action."""
        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=self.console
        ) as progress:
            progress.add_task(f"{source}: {message}", total=None)

    def warnings(self, results: Iterable[SourceResult]) -> None:
        """Render warning results in red."""
        for result in results:
            if result.is_warning:
                day = f" {result.day}" if result.day else ""
                message = result.message or result.status.value
                self.console.print(
                    f"WARNING {result.source}{day}: {message}; {result.status.value}",
                    style="red",
                    highlight=False,
                )

    def summary(
        self,
        summary: Summary,
        output_dir: Path,
        manifest_path: Path,
        *,
        title: str = "collekt collection complete",
        manifest_written: bool = True,
    ) -> None:
        """Render the final collection summary."""
        manifest_text = manifest_path.name if manifest_written else "not written"
        text = (
            f"Planned: {summary.planned}  Downloaded: {summary.downloaded}  Reused: {summary.reused}  "
            f"Skipped: {summary.skipped}  Failed: {summary.failed}\n"
            f"Output: {output_dir}\n"
            f"Manifest: {manifest_text}"
        )
        self.console.print(Panel(text, title=title, border_style="green"))
