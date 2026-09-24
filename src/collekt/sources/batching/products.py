"""Date-batched catalogue searches with individual product downloads."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

from collekt.core.batching import request_windows
from collekt.core.config import Config, SourceConfig
from collekt.core.request import Request
from collekt.sources.base import BatchOptions, SourceResult, SourceStatus, should_reuse_cache
from collekt.sources.batching.common import batch_details, checkpoint_query, failed


def product_batch_errors(source: SourceConfig) -> list[str]:
    """Report configurations the windowed catalogue search cannot honour."""
    try:
        cap = int(source.raw.get("max_records", 100))
    except (TypeError, ValueError):
        return [f"max_records must be an integer, not {source.raw.get('max_records')!r}"]
    return [] if cap >= 1 else ["max_records must be positive for batch downloads"]


def run_product_batch(
    request: Request,
    source: SourceConfig,
    config: Config,
    request_dir: Path,
    options: BatchOptions,
) -> list[SourceResult]:
    """Search windows, paginate, and apply max_records once across unique products."""
    from collekt.sources import copernicus_dataspace as cds

    collection = source.raw.get("collection") or source.dataset_id
    base = cds.plan_copernicus_dataspace(request, source, config, request_dir)[0]
    if not collection:
        return [failed(base, "no collection configured for source")]
    windows = request_windows(request, options.days)
    batches = [
        batch_details(
            source,
            window.start_datetime,
            window.end_datetime,
            cds._search_params(window, source, collection),
            transport="search_then_individual_files",
        )
        for window in windows
    ]
    for batch, following in zip(batches, windows[1:], strict=False):
        start = batch["request"]["datetime"].split("/", 1)[0]
        batch["request"]["datetime"] = f"{start}/{following.start_datetime.isoformat(timespec='seconds')}"
    batches = [
        batch_details(source, batch["start"], batch["end"], batch["request"], transport="search_then_individual_files")
        for batch in batches
    ]
    cap = int(source.raw.get("max_records", 100))
    if options.dry_run:
        return [replace(base, details=base.details | {"batches": batches, "max_records": cap})]
    out_dir = request_dir / source.path
    out_dir.mkdir(parents=True, exist_ok=True)
    token = None

    def access_token() -> str:
        nonlocal token
        if token is None:
            token = cds._login(*cds._credentials())
        return token

    results = []
    seen = set()
    for batch in batches:
        if len(seen) >= cap:
            break
        item = replace(base, details=base.details | {"batch": batch})
        options.progress(source.name, f"searching batch {batch['start']} to {batch['end']}")
        try:
            features = checkpoint_query(
                out_dir / ".batch-searches",
                batch,
                config,
                lambda batch=batch: _search_pages(access_token(), batch["request"], cap),
            )
        except Exception as exc:  # noqa: BLE001 - continue independent search windows
            results.append(failed(item, exc))
            continue
        for feature in features:
            product = (feature.get("assets") or {}).get("Product") or {}
            identity = feature.get("id") or product.get("href")
            if identity is None or identity in seen:
                continue
            if len(seen) >= cap:
                break
            seen.add(identity)
            if not product.get("href"):
                continue
            name = product.get("file:local_path") or str(identity)
            path = out_dir / name
            if not path.resolve().is_relative_to(out_dir.resolve()):
                results.append(failed(item, f"product path is outside the output directory: {name}"))
                continue
            product_result = replace(item, path=path, format=cds._format_for(name))
            if should_reuse_cache(path.exists(), config):
                results.append(replace(product_result, status=SourceStatus.REUSED, details=base.details))
                continue
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                cds._download(product["href"], access_token(), path)
                if not path.is_file():
                    raise RuntimeError("download completed but file is missing")
                results.append(replace(product_result, status=SourceStatus.DOWNLOADED))
            except Exception as exc:  # noqa: BLE001 - preserve successful products
                results.append(failed(product_result, exc))
    return results or [failed(base, "no downloadable products for this query")]


def _search_pages(token: str, params: dict[str, Any], cap: int) -> list[dict[str, Any]]:
    """Follow STAC GET next links without forwarding credentials to another host."""
    from collekt.sources import copernicus_dataspace as cds

    catalogue = cds._search(token, params)
    features = []
    seen = set()
    visited = set()
    while True:
        for feature in catalogue.get("features", []):
            identity = feature.get("id")
            if identity is None or identity not in seen:
                features.append(feature)
                if identity is not None:
                    seen.add(identity)
            if len(features) >= cap:
                return features
        link = next((link for link in catalogue.get("links", []) if link.get("rel") == "next"), None)
        if link is None:
            return features
        url = urljoin(cds.SEARCH_URL, link["href"])
        if url in visited:
            raise ValueError("catalogue pagination repeated a next link")
        if urlsplit(url).netloc != urlsplit(cds.SEARCH_URL).netloc or urlsplit(url).scheme != "https":
            raise ValueError("catalogue next link is outside the authenticated catalogue")
        if link.get("method", "GET").upper() != "GET":
            raise ValueError("catalogue pagination requires an unsupported method")
        visited.add(url)
        response = cds.requests.get(url, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        catalogue = response.json()
