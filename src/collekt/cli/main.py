"""Command-line interface for collekt."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from rich.console import Console
from rich.table import Table

from collekt.core.config import LOG_DATE_FORMAT, LOG_FORMAT, LOG_STYLE, load_config
from collekt.core.datasets import DatasetConfig
from collekt.core.doctor import run_doctor
from collekt.core.fetcher import Fetcher
from collekt.core.reporting import CliReporter
from collekt.core.request import Region, Request
from collekt.sources.base import SourceResult


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


def _render_doctor(console: Console, conf_dir: Path | None, *, online: bool = False) -> int:
    checks = run_doctor(conf_dir=conf_dir, online=online)
    table = Table(title="collekt doctor")
    table.add_column("Status")
    table.add_column("Check")
    table.add_column("Message")
    status_styles = {"ok": "green", "warn": "yellow", "fail": "red"}
    for check in checks:
        table.add_row(f"[{status_styles.get(check.status, 'white')}]{check.status}[/]", check.name, check.message)
    console.print(table)
    return 1 if any(check.status == "fail" for check in checks) else 0


def build_parser() -> argparse.ArgumentParser:
    """Build the collekt argument parser."""
    parser = argparse.ArgumentParser(prog="collekt", description="Collect spatio-temporal data from arbitrary sources")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch", help="Fetch source-native data for a region and time window")
    fetch.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "EAST", "SOUTH", "NORTH"))
    fetch.add_argument("--at-lat", type=float, help="Centre latitude (with --at-lon and --radius)")
    fetch.add_argument("--at-lon", type=float, help="Centre longitude (with --at-lat and --radius)")
    fetch.add_argument("--radius", type=float, help="Radius in km around the centre point")
    fetch.add_argument("--region", type=Path, help="GeoJSON file describing the region")
    fetch.add_argument("--start", help="Start date/datetime; defaults to current UTC day")
    fetch.add_argument("--end", help="End date/datetime; defaults to the start day")
    fetch.add_argument(
        "--dataset-config",
        type=Path,
        required=True,
        help="YAML file containing a DatasetConfig datasets list",
    )
    fetch.add_argument("--output-dir", type=Path, required=True, help="Staging root for downloaded files")
    fetch.add_argument("--strict", action="store_true", help="Fail if any requested source is skipped")
    fetch.add_argument("--no-cache", action="store_true", help="Ignore cached files and download again")
    fetch.add_argument("--dry-run", action="store_true", help="Plan provider requests without downloading")

    doctor = subparsers.add_parser("doctor", help="Check the local environment and source configuration")
    doctor.add_argument("--conf-dir", type=Path)
    doctor.add_argument("--online", action="store_true", help="Validate CMEMS datasets and variables online")

    config = subparsers.add_parser("config", help="Inspect configuration")
    config_subparsers = config.add_subparsers(dest="config_command", required=True)
    show = config_subparsers.add_parser("show", help="Print merged configuration")
    show.add_argument("--conf-dir", type=Path)

    return parser


def run(argv: list[str] | None = None) -> int:
    """Run the collekt command-line interface."""
    logging.basicConfig(format=LOG_FORMAT, style=LOG_STYLE, datefmt=LOG_DATE_FORMAT, level=logging.INFO)
    parser = build_parser()
    args = parser.parse_args(argv)
    console = Console()

    if args.command == "config" and args.config_command == "show":
        import yaml

        console.print(yaml.safe_dump(load_config(conf_dir=args.conf_dir), sort_keys=False))
        return 0

    if args.command == "doctor":
        return _render_doctor(console, args.conf_dir, online=args.online)

    if args.command == "fetch":
        reporter = CliReporter(console)
        try:
            request = Request(
                region=_build_region(args),
                start=args.start,
                end=args.end,
            )
            fetcher = Fetcher(
                request=request,
                config=DatasetConfig.from_yaml(args.dataset_config),
                output_dir=args.output_dir,
                strict=args.strict,
                progress=reporter.progress,
            )
            result = fetcher.plan() if args.dry_run else fetcher.download(use_cache=not args.no_cache)
        except (RuntimeError, ValueError) as exc:
            console.print(f"ERROR {exc}", style="red")
            return 1
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

    parser.error(f"unknown command {args.command!r}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
