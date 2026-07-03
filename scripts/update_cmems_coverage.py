#!/usr/bin/env python3
"""Refresh declared CMEMS coverage in bundled source catalogues.

This script is intentionally manual: run it from the repository root when the
shipped coverage hints should be refreshed from the Copernicus Marine catalogue.
It updates only ``coverage:`` blocks for sources in ``src/collekt/conf/source``
whose filenames start with ``cmems_``.

Usage:
    uv run python scripts/update_cmems_coverage.py
    uv run python scripts/update_cmems_coverage.py --dry-run
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from collekt.core.availability import Coverage, _coverage_from_catalogue

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = REPO_ROOT / "src" / "collekt" / "conf" / "source"


def _copernicusmarine_describe():
    try:
        import copernicusmarine
    except ImportError as exc:
        raise SystemExit("copernicusmarine is required; run this through `uv run`.") from exc
    return copernicusmarine.describe


def _iter_cmems_catalogues(source_dir: Path) -> Iterable[Path]:
    return sorted(source_dir.glob("cmems_*.yaml"))


def _iter_cmems_sources(catalogue: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for name, source in (catalogue.get("sources") or {}).items():
        if isinstance(source, dict) and source.get("kind") == "cmems" and source.get("dataset_id"):
            yield str(name), source


def _coverage_for_dataset(describe, dataset_id: str) -> Coverage | None:
    try:
        catalogue = describe(dataset_id=dataset_id, disable_progress_bar=True, raise_on_error=True)
    except Exception as exc:  # noqa: BLE001 - this is a manual maintenance script; keep going.
        print(f"WARN {dataset_id}: describe failed: {exc}")
        return None
    coverage = _coverage_from_catalogue(catalogue, dataset_id)
    if coverage is None:
        print(f"WARN {dataset_id}: dataset not found in catalogue response")
    return coverage


def _coverage_block(source_name: str, coverage: Coverage) -> list[str]:
    kind = "archive" if source_name.endswith("_my") else "rolling"
    end = f'"{coverage.end.isoformat()}"' if kind == "archive" and coverage.end else "null"
    return [
        "    coverage:\n",
        f"      longitude: [{_fmt(coverage.west)}, {_fmt(coverage.east)}]\n",
        f"      latitude: [{_fmt(coverage.south)}, {_fmt(coverage.north)}]\n",
        "      temporal:\n",
        f'        start: "{coverage.start.isoformat()}"\n' if coverage.start else "        start: null\n",
        f"        end: {end}\n",
        f"        kind: {kind}\n",
    ]


def _fmt(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _source_ranges(lines: list[str], source_names: Iterable[str]) -> dict[str, tuple[int, int]]:
    names = set(source_names)
    headers: list[tuple[str, int]] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
            name = stripped[:-1]
            if name in names:
                headers.append((name, index))
    ranges: dict[str, tuple[int, int]] = {}
    for position, (name, start) in enumerate(headers):
        end = headers[position + 1][1] if position + 1 < len(headers) else len(lines)
        ranges[name] = (start, end)
    return ranges


def _replace_coverage(lines: list[str], block: list[str], source_range: tuple[int, int]) -> bool:
    start, end = source_range
    coverage_start = None
    coverage_end = None
    for index in range(start + 1, end):
        if lines[index].startswith("    coverage:"):
            coverage_start = index
            coverage_end = index + 1
            while coverage_end < end and (lines[coverage_end].startswith("      ") or not lines[coverage_end].strip()):
                coverage_end += 1
            break
    if coverage_start is None:
        insert_at = _coverage_insert_index(lines, start, end)
        lines[insert_at:insert_at] = block
        return True
    if lines[coverage_start:coverage_end] == block:
        return False
    lines[coverage_start:coverage_end] = block
    return True


def _coverage_insert_index(lines: list[str], start: int, end: int) -> int:
    for index in range(start + 1, end):
        stripped = lines[index].strip()
        if stripped.startswith("coordinates_selection_method:") or stripped.startswith("time_selection:"):
            return index
    for index in range(start + 1, end):
        if lines[index].strip().startswith("temporal_sampling:"):
            return index + 1
    return start + 1


def update_catalogue(path: Path, *, describe, dry_run: bool) -> list[str]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    sources = dict(_iter_cmems_sources(data))
    if not sources:
        return []
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    ranges = _source_ranges(lines, sources)
    updates: list[str] = []
    for name, source in sources.items():
        dataset_id = str(source["dataset_id"])
        coverage = _coverage_for_dataset(describe, dataset_id)
        if coverage is None:
            continue
        if name not in ranges:
            print(f"WARN {path.name}:{name}: source block not found in YAML text")
            continue
        if _replace_coverage(lines, _coverage_block(name, coverage), ranges[name]):
            ranges = _source_ranges(lines, sources)
            updates.append(name)
    if updates and not dry_run:
        path.write_text("".join(lines), encoding="utf-8")
    return updates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=SOURCE_DIR, help="Directory containing cmems_*.yaml files.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report updates without writing files.")
    args = parser.parse_args()

    describe = _copernicusmarine_describe()
    total = 0
    for path in _iter_cmems_catalogues(args.source_dir):
        updated = update_catalogue(path, describe=describe, dry_run=args.dry_run)
        if updated:
            action = "would update" if args.dry_run else "updated"
            print(f"{action} {len(updated)} coverage block(s) in {path}: {', '.join(updated)}")
        total += len(updated)
    print(f"{'would update' if args.dry_run else 'updated'} {total} CMEMS coverage block(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
