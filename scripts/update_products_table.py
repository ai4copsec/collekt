#!/usr/bin/env python3
"""Regenerate the CMEMS tables in ``docs/products.qmd`` from the bundled catalog.

Both tables in the documentation are projections of
``src/collekt/conf/source/cmems_*.yaml``: the per-catalog source list under
"Bundled catalog", and the dated snapshot under "CMEMS Dataset Snapshot". This
script rewrites them in place so they cannot drift from the catalog.

Run it after adding a source or refreshing coverage with
``scripts/update_cmems_coverage.py``; it reads only local files.

Usage:
    uv run python scripts/update_products_table.py
    uv run python scripts/update_products_table.py --dry-run
"""

from __future__ import annotations

import argparse
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = REPO_ROOT / "src" / "collekt" / "conf" / "source"
PRODUCTS_QMD = REPO_ROOT / "docs" / "products.qmd"

# Catalog file stem -> snapshot tab title, in the order the tabs appear.
CMEMS_TABS: dict[str, str] = {
    "cmems_global": "Global",
    "cmems_eur": "European seas",
    "cmems_ibi": "IBI / Atlantic",
    "cmems_med": "Mediterranean",
    "cmems_nws": "North-West Shelf",
}

HEADER = (
    "| Source key | Dataset ID | Resolution | Native sampling | Spatial coverage "
    "| Temporal coverage | Variables | Depth | DOI / product |\n"
    "|---|---|---|---|---|---|---|---|---|\n"
)

# A CMEMS dataset id encodes its grid as e.g. `0.083deg`, `4.2km` or `1.5km`.
_RESOLUTION = re.compile(r"[_-](\d+(?:\.\d+)?)(deg|km)(?=[_-]|$)")

_TABSET_OPEN = "::: {.panel-tabset .cmems-catalog}\n"
_TABSET_CLOSE = ":::\n"


def load_catalogs(source_dir: Path) -> dict[str, dict[str, dict[str, Any]]]:
    """Return ``{catalog stem: {source name: source mapping}}`` for CMEMS sources."""
    catalogs: dict[str, dict[str, dict[str, Any]]] = {}
    for stem in CMEMS_TABS:
        path = source_dir / f"{stem}.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        catalogs[stem] = {
            str(name): source
            for name, source in (data.get("sources") or {}).items()
            if isinstance(source, dict) and source.get("kind") == "cmems"
        }
    return catalogs


def _resolution(dataset_id: str) -> str:
    match = _RESOLUTION.search(dataset_id)
    return f"{match.group(1)} {match.group(2)}" if match else "not declared"


def _spatial(source: dict[str, Any]) -> str:
    coverage = source.get("coverage") or {}
    west, east = coverage.get("longitude", [None, None])
    south, north = coverage.get("latitude", [None, None])
    if None in (west, east, south, north):
        return "not declared"
    return f"{float(west):.1f}/{float(east):.1f} {float(south):.1f}/{float(north):.1f}"


def _temporal(source: dict[str, Any], snapshot: str) -> str:
    temporal = (source.get("coverage") or {}).get("temporal") or {}
    start, end = temporal.get("start"), temporal.get("end")
    if not start:
        return "not declared"
    if end:
        return f"`{start}` to `{end}`"
    return f"from `{start}` (rolling/open on {snapshot})"


def _row(name: str, source: dict[str, Any], snapshot: str) -> str:
    dataset_id = str(source.get("dataset_id", ""))
    variables = ", ".join(f"`{variable}`" for variable in source.get("available_variables") or [])
    doi = source.get("doi")
    links = (
        f"[{doi}](https://doi.org/{doi})<br>[product]({source.get('url')})"
        if doi
        else f"[product]({source.get('url')})"
    )
    return (
        f"| `{name}` | `{dataset_id}` | {_resolution(dataset_id)} | `{source.get('temporal_sampling')}` "
        f"| {_spatial(source)} | {_temporal(source, snapshot)} | {variables} "
        f"| {'yes' if source.get('has_depth') else 'no'} | {links} |\n"
    )


def render_snapshot(catalogs: dict[str, dict[str, dict[str, Any]]], snapshot: str) -> str:
    """Return the full ``.panel-tabset`` block, tabs included."""
    parts = [_TABSET_OPEN]
    for stem, title in CMEMS_TABS.items():
        sources = catalogs[stem]
        if not sources:
            continue
        parts.append(f"\n### {title}\n\n{HEADER}")
        parts.extend(_row(name, source, snapshot) for name, source in sources.items())
    parts.append("\n" + _TABSET_CLOSE)
    return "".join(parts)


def render_catalog_rows(catalogs: dict[str, dict[str, dict[str, Any]]]) -> dict[str, str]:
    """Return ``{catalog stem: table row}`` for the "Bundled catalog" table."""
    return {
        stem: f"| `{stem}` | {', '.join(f'`{name}`' for name in sources)} |\n"
        for stem, sources in catalogs.items()
        if sources
    }


def _replace_snapshot(text: str, block: str) -> str:
    start = text.index(_TABSET_OPEN)
    end = text.index(f"\n{_TABSET_CLOSE}", start) + len(_TABSET_CLOSE) + 1
    return text[:start] + block + text[end:]


def _replace_catalog_rows(text: str, rows: dict[str, str]) -> str:
    for stem, row in rows.items():
        pattern = re.compile(rf"^\| `{re.escape(stem)}` \| .*\n", re.MULTILINE)
        if pattern.search(text) is None:
            raise SystemExit(f"no 'Bundled catalog' row for {stem} in {PRODUCTS_QMD.name}")
        text = pattern.sub(lambda _, row=row: row, text, count=1)
    return text


def _replace_snapshot_date(text: str, snapshot: str) -> str:
    return re.sub(r"catalog files as of \d{4}-\d{2}-\d{2}\.", f"catalog files as of {snapshot}.", text, count=1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=SOURCE_DIR, help="Directory containing cmems_*.yaml files.")
    parser.add_argument("--products", type=Path, default=PRODUCTS_QMD, help="Path to docs/products.qmd.")
    parser.add_argument("--dry-run", action="store_true", help="Report whether the tables change without writing.")
    args = parser.parse_args()

    snapshot = datetime.now(UTC).date().isoformat()
    catalogs = load_catalogs(args.source_dir)
    original = args.products.read_text(encoding="utf-8")

    updated = _replace_snapshot(original, render_snapshot(catalogs, snapshot))
    updated = _replace_catalog_rows(updated, render_catalog_rows(catalogs))
    updated = _replace_snapshot_date(updated, snapshot)

    if updated == original:
        print(f"{args.products} is up to date")
        return 0
    if not args.dry_run:
        args.products.write_text(updated, encoding="utf-8")
    print(f"{'would update' if args.dry_run else 'updated'} {args.products}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
