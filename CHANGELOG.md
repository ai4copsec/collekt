# Changelog

All notable changes to collekt are documented here. Unreleased changes live
under `[main]`; `just bump` copies them under the new version.

## [main]

### Added

- Opt-in `Fetcher(..., batch_days=N)` and `collekt fetch --batch-days N` for
  provider-aware retrieval groups across all ten built-in source kinds. ERA5
  and compatible CMEMS requests are split back into daily NetCDF files;
  forecast runs, event metadata, global record caps, and existing filenames
  are preserved. Batch plans/provenance distinguish retrieval groups from
  output files, with resumable event queries and isolated HOZINT windows.
  The default (`None`) keeps the existing download behavior.

- a `local` source adapter and `Local` `DatasetSelection`: relies on damast
  to interface an existing tabular-data archive (file(s) *.csv, *.parquet).
  Data can be queried through the normal `Request(region, time)` -> `fetch()` -> manifest
  pipeline, like any remote source.
  This is the tabular generalization of eOdyn's `mode: archive`.  Configured
  per source with `archive_root`, `layout` (a `damast`-style `--save-as`
  partitioning spec - `damast.core.SaveAs.expected_paths` resolves it back to
  candidate files for a request's time range, so the naming scheme is shared with
  `damast` rather than re-derived), and `region_columns` (the lat/lon columns
  used to row-filter each matched file, since damast does not offer a file
  partitioning strategy for a spatial dimension).
  It must, however, carry a time dimension (`time:`/`time+column:`) - the column
  it partitions on is also what scopes each request day, which a `column:`-only
  or plain-path layout has no way to provide. Requires a `damast` release
  carrying `SaveAs.expected_paths`. There is no bundled catalog entry - the
  archive location is inherently deployment-specific, so a `local` source is
  always reached via `conf_dir`, same as any other downstream-only dataset.
  An unpartitioned archive - no per-day filenames at all - is supported as an
  alternative to `layout`: `file_pattern` (a glob, resolved once per fetch) and
  `time_column` select a day's rows by filtering the column directly, the same
  way `region_columns` already does for region; `layout` and `file_pattern` are
  mutually exclusive.
  Rows are always filtered by both time and region, so `layout` only prunes which
  files are opened: a partitioning coarser than the request (monthly, say) still
  yields one day's rows per day rather than repeating the whole bucket. A layout
  whose files carry a compression suffix (`<name>.zst.parquet`) is matched too,
  and a `region_columns`/`time_column` entry the archive does not have is raised
  as a configuration error instead of skipping every day as if it held no data.
  Only Parquet archives are read lazily; damast's NetCDF, CSV and HDF readers
  load each file in full.

- `gfw` source adapter and catalog entry: Global Fishing Watch Events API
  (fishing, port visits, encounters, loitering, AIS gaps), filtered by the
  request's region (as a GeoJSON geometry, not a GFW named region id) and time
  window, written as one annotated Parquet file per request. Event-type-specific
  detail (`encounter`/`fishing`/`gap`/`loitering`/`port_visit`) is kept as JSON
  text rather than a typed struct, since GFW's response models allow
  undocumented extra fields. The query pages on `offset` until GFW runs out of
  events, so a result is complete by default: `limit` is the per-request page
  size (unset = the client's 99999, keeping the common case to a single request,
  since a query costs 10-20s server-side almost regardless of row count), and the
  optional `max_events` caps the total across pages, raising
  `GFWTruncatedResultWarning` rather than truncating silently. Public `GFW`
  `DatasetConfig` dataclass.

- `gfs` source adapter and `gfs_analysis` catalog entry: NOAA GFS analysis wind
  from the NOMADS GRIB filter, with server-side region subsetting, one NetCDF per
  day holding that day's available analysis cycles, and heights selected as
  `{height}u`/`{height}v` mnemonics. Cycles the provider has not published yet are
  skipped rather than failing the day.
- Subdaily CMEMS currents entries, so every model-currents product in the
  catalog now offers its native cadences and not only the daily mean. Each maps
  to one provider dataset and one cadence, and shares its `path`, grid and
  region with the daily sibling, so switching is a one-key change:
  - `cmems_med_currents_my_2d_hourly` (`cmems_mod_med_phy-cur_my_4.2km_PT1H-m`) —
    hourly Mediterranean multi-year reanalysis, next to daily
    `cmems_med_currents_my`.
  - `cmems_ibi_currents_2d_hourly` / `cmems_ibi_currents_3d_hourly` — hourly IBI
    analysis/forecast, surface and depth-resolved.
  - `cmems_nws_currents_2d_hourly` / `cmems_nws_currents_3d_hourly` — hourly
    North-West Shelf analysis/forecast, surface and depth-resolved.
  - `cmems_glorys_nrt_2d_hourly` (hourly global surface fields),
    `cmems_glorys_nrt_3d_6h` (6-hourly depth-resolved currents), and
    `cmems_glorys_nrt_total_currents` (hourly total surface currents, adding
    tide and Stokes-drift components to the model's own `uo`/`vo`).

  Naming follows one rule: the daily mean is the unsuffixed base entry and every
  subdaily variant of it carries both its dimensionality and its cadence, since
  dimensionality is what decides whether a caller must pass a `depth`. The
  released `cmems_med_currents_nrt_15min` predates the rule and keeps its name.

  `has_depth` now consistently means a real depth *selection*: CMEMS ships its
  `-2D` and `merged-uv` datasets with a single degenerate depth level, and those
  declare `has_depth: false` rather than asking a caller to pick a range out of
  one value.
- `scripts/update_products_table.py`, which regenerates both CMEMS tables in
  `docs/products.qmd` from the bundled catalog, so a 35-row derived table cannot
  drift from the YAML it describes.
- Catalog entries for the European high-resolution DUACS sea-level products
  (`cmems_duacs_eur_nrt` / `cmems_duacs_eur_my`, 0.0625°) in a new `cmems_eur`
  source catalog, and for the CMEMS hourly L4 near-real-time global wind
  (`cmems_global_wind_nrt`).
- Bundled source catalogs for CMEMS, ECMWF, eOdyn, SkyTruth, HOZINT, and
  Copernicus Data Space, including CMEMS Global/MED/IBI/NWS metadata and coverage
  declarations. Every first-class provider has a `DatasetConfig` dataclass
  (`CMEMS`, `ERA5`, `ECMWFOpenData`, `Eodyn`, `SkyTruth`, `Hozint`,
  `CopernicusDataSpace`).
- Manual `scripts/update_cmems_coverage.py` helper to refresh declared CMEMS
  coverage from the Copernicus catalogue.
- Public `available_datasets`/`describe_datasets` to browse the bundled (and any
  downstream-overlaid) catalog programmatically, without reading the source YAML.
- Downstream catalog extension: a `conf_dir` overlays the bundled configuration
  (adding or overriding datasets and appending `source_catalogs`, so the shipped
  datasets stay available), threaded through `collekt fetch --conf-dir`,
  `Fetcher(conf_dir=…)`, and `DatasetConfig.resolve(conf_dir=…)`.
- Project tooling: uv + `uv_build`, Ruff, commitizen, a `justfile`, GitHub
  Actions CI, and a Quarto + quartodoc documentation site.
- Typed request model: `Request` and `Region` (from bounding box, point+radius,
  or GeoJSON). Each dataset is fetched at its native cadence (`temporal_sampling`);
  there is no request-level sampling — thinning/aligning is a downstream
  `Assembler` step.
- Layered YAML configuration mechanism (`conf_dir`, source catalogs) plus
  public `DatasetConfig` YAML presets for selecting dataset keys and provider
  parameters.
- Cache-aware collection orchestration with a pluggable source-adapter registry,
  non-blocking per-source warnings, a strict mode, and a versioned, relocatable
  `manifest.json` (with post-fetch file inspection and a draft schema).
- Public `Fetcher` workflow (`check` / `plan` / `download` / directory & zip
  export) and `Result`, with `output_dir` owned by `Fetcher`.
- Source adapters: `cmems`, `ecmwf_open_data`, `era5`, `skytruth`, `hozint`,
  `copernicus_dataspace`, and `eodyn` (archive interim mode with an API seam).
- Gridded `Assembler`: regrid onto a common grid and align/aggregate time, with
  xarray / NetCDF / Zarr / NumPy output.
- `collekt fetch`, `collekt doctor`, and `collekt config show` command-line
  interface, plus `run_doctor` / `DoctorCheck` diagnostics.

### Removed

- Vestigial `supported_sampling` source metadata; datasets now declare only their
  native `temporal_sampling`.
- Request-level variables, variable groups, `use_variables`, per-source variable
  overrides, and fetch-time datasource filters; selected variables now live on
  provider dataset declarations.
- Legacy `collekt query` CLI, `Collector`, `Query`, the `DataSource` base classes,
  and the old `datasources/` package (superseded by the adapter framework).
- Unused `core/credentials.py`: every credential-gated adapter already resolves
  its own credentials.

### Changed

- Source collection now validates explicit variable/depth selections for bundled
  catalogs and only allows request sampling at dataset cadence or coarser
  integer multiples.
- The "a source needs a variable/depth selection" check had three independent
  implementations (`collect.py`, `datasets.py`, `config/schema.py`); it is now
  one shared `collekt.core.config.selection_error`, called from both
  `DatasetConfig.resolve()` and `run_collection`.
- `collekt doctor` now warns about source config keys not recognized by their
  adapter (e.g. `pad_dg` instead of `pad_deg`), which were previously ignored
  with no error or warning — the adapter just silently fell back to its default.
- Every fetch adapter repeated the same cache-hit check
  (`output_path.exists() and config.cache.reuse_existing and not
  config.cache.overwrite`), and cmems/era5/ecmwf_open_data each repeated the
  same "download completed but file is missing" SKIPPED result verbatim.
  Both are now shared helpers (`should_reuse_cache`, `missing_after_fetch` in
  `sources/base.py`) used by all seven source adapters.
- CMEMS diagnostics and planning use declared coverage metadata as an offline
  fallback, with rolling NRT coverage handled separately from archive products.
- Example notebooks and source documentation were refreshed for the bundled
  catalogs and native-resolution CMEMS inspection workflow.

### Fixed

- Declared CMEMS coverage refreshed against the Copernicus Marine catalogue for
  14 entries whose start or end had drifted (`cmems_glorys_my`,
  `cmems_global_sst_my`, the global/Atlantic/Mediterranean ocean-colour sources,
  the Mediterranean NRT currents, and the North-West Shelf currents, waves and
  BGC sources). `collekt doctor --online` now reports no CMEMS coverage or
  variable mismatches.
- `docs/products.qmd` was missing the `cmems_eur` catalog from both its catalog
  listing and its dataset snapshot.
- `scripts/update_cmems_coverage.py` classified a source as a multi-year archive
  only when its name ended in `_my`, so refreshing a source such as
  `cmems_med_currents_my_2d_hourly` would have rewritten its coverage as
  `kind: rolling` with an open `end`, letting offline planning accept days past
  the end of the archive. `_my` is now matched as a name segment.
- ECMWF Open Data always serves the full global grid (no server-side region
  subsetting). The adapter now crops downloaded GRIB2 files to the padded
  request region and writes them out as NetCDF, matching the region-scoped
  output of the other gridded adapters (`cmems`, `era5`).
- ERA5 and Copernicus Data Space downloads now stream to a temporary file and
  only move it into place after completing fully, so a dropped connection can
  no longer leave a partial file that a later run mistakes for a valid,
  already-downloaded product.
- `copernicus_dataspace` and `skytruth` now degrade to a per-source warning
  instead of aborting the whole collection run on a malformed API response or
  a missing optional dependency (e.g. `damast`/`geopandas` for skytruth).
- ECMWF Open Data: `10fg` (10m wind gust, a running maximum) shares a GRIB2 file
  with instantaneous fields (`10u`/`10v`/`100u`/`100v`); cfgrib cannot merge the
  two into one hypercube and previously dropped `10fg` from the cropped NetCDF
  with no error or warning. The adapter now opens each cfgrib hypercube
  separately, merges them, and renames variables back to their requested
  mnemonics; any param still missing from the result is reported in the
  source's manifest message instead of silently vanishing.
