"""`collekt fetch` — download source-native data for a region and time window."""

from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console
from rich.table import Table

from collekt.core.datasets import DatasetConfig
from collekt.core.fetcher import Fetcher
from collekt.core.reporting import CliReporter
from collekt.core.request import Region, Request
from collekt.sources.base import SourceResult, SourceStatus


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``fetch`` subcommand."""
    parser = subparsers.add_parser("fetch", help="Fetch source-native data for a region and time window")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "EAST", "SOUTH", "NORTH"))
    parser.add_argument("--at-lat", type=float, help="Centre latitude (with --at-lon and --radius)")
    parser.add_argument("--at-lon", type=float, help="Centre longitude (with --at-lat and --radius)")
    parser.add_argument("--radius", type=float, help="Radius in km around the centre point")
    parser.add_argument("--region", type=Path, help="GeoJSON file describing the region")
    parser.add_argument("--start", help="Start date/datetime; defaults to current UTC day")
    parser.add_argument("--end", help="End date/datetime; defaults to the start day")
    parser.add_argument(
        "--dataset-config",
        type=Path,
        required=True,
        help="YAML file containing a DatasetConfig datasets list",
    )
    parser.add_argument(
        "--conf-dir",
        type=Path,
        help="Configuration directory that adds or overrides datasets on top of the bundled catalog",
    )
    parser.add_argument("--output-dir", type=Path, required=True, help="Staging root for downloaded files")
    parser.add_argument("--strict", action="store_true", help="Fail if any requested source is skipped")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached files and download again")
    parser.add_argument("--dry-run", action="store_true", help="Plan provider requests without downloading")
    parser.add_argument(
        "--batch-days", type=int, help="Maximum UTC calendar days per retrieval group (positive integer)"
    )
    parser.set_defaults(handler=execute)


def execute(args: argparse.Namespace, console: Console) -> int:
    """Build the request, run the fetch workflow, and report the outcome."""
    reporter = CliReporter(console)
    request = Request(region=_build_region(args), start=args.start, end=args.end)
    fetcher = Fetcher(
        request=request,
        config=DatasetConfig.from_yaml(args.dataset_config),
        output_dir=args.output_dir,
        conf_dir=args.conf_dir,
        strict=args.strict,
        progress=reporter.progress,
        batch_days=args.batch_days,
    )
    result = fetcher.plan() if args.dry_run else fetcher.download(use_cache=not args.no_cache)
    if args.dry_run:
        _render_plan(console, result.results)
        reporter.summary(
            result.summary,
            result.output_dir,
            result.manifest_path,
            title="collekt dry run complete",
            manifest_written=False,
        )
        return 0
    reporter.warnings(result.results)
    reporter.summary(result.summary, result.output_dir, result.manifest_path)
    return 0


def _build_region(args: argparse.Namespace) -> Region:
    if args.bbox:
        return Region.from_bbox(args.bbox)
    if args.at_lat is not None and args.at_lon is not None and args.radius is not None:
        return Region.from_point_radius(args.at_lat, args.at_lon, args.radius)
    if args.region:
        return Region.from_geojson(args.region)
    raise ValueError("provide a region: --bbox W E S N, --at-lat/--at-lon/--radius, or --region <geojson>")


def _render_plan(console: Console, results: tuple[SourceResult, ...]) -> None:
    table = Table(title="collekt dry-run plan")
    table.add_column("Source")
    table.add_column("Day")
    table.add_column("Available")
    table.add_column("Sampling")
    table.add_column("Dataset")
    table.add_column("Variables")
    table.add_column("Output")
    styles = {"available": "green", "unavailable": "red", "unknown": "yellow", "not_checked": "yellow"}
    for result in results:
        details = result.details or {}
        temporal = details.get("temporal") or {}
        availability = (details.get("availability") or {}).get("status", "")
        available_cell = f"[{styles[availability]}]{availability}[/]" if availability in styles else availability
        table.add_row(
            result.source,
            result.day or "",
            available_cell,
            str(temporal.get("actual_sampling", "")),
            result.dataset_id or "",
            ", ".join(result.variables),
            str(result.path or ""),
        )
    console.print(table)
    groups = {}
    for result in results:
        if result.status != SourceStatus.PLANNED:
            continue
        details = result.details or {}
        batches = [details["batch"]] if "batch" in details else details.get("batches", [])
        for batch in batches:
            groups[(result.source, batch["id"])] = batch
    if groups:
        table = Table(title=f"{len(groups)} planned retrieval groups (provider requests may be multiple)")
        for name in ("Source", "Start", "End", "Transport"):
            table.add_column(name)
        for (source, _), batch in groups.items():
            table.add_row(source, batch["start"], batch["end"], batch["transport"])
        console.print(table)
