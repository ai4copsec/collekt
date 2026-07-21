# Changelog

All notable changes to collekt are documented here. Unreleased changes live
under `[main]`; `just bump` copies them under the new version.

## [main]

### Added

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
