# Changelog

All notable changes to collekt are documented here. Unreleased changes live
under `[main]`; `just bump` copies them under the new version.

## [main]

### Added

- Bundled source catalogs for CMEMS, ECMWF, eOdyn, SkyTruth, and Copernicus Data
  Space, including CMEMS Global/MED/IBI/NWS metadata and coverage declarations.
- Manual `scripts/update_cmems_coverage.py` helper to refresh declared CMEMS
  coverage from the Copernicus catalogue.
- Project tooling: uv + `uv_build`, Ruff, commitizen, a `justfile`, GitHub
  Actions CI, and a Quarto + quartodoc documentation site.
- Typed request model: `Request` and `Region` (from bounding box, point+radius,
  or GeoJSON), with temporal sampling (`1h`/`3h`/`6h`/`24h`).
- Layered YAML configuration mechanism (`conf_dir`, source catalogs, presets,
  per-source variable resolution and overrides).
- Cache-aware collection orchestration with a pluggable source-adapter registry,
  non-blocking per-source warnings, a strict mode, and a versioned, relocatable
  `manifest.json` (with post-fetch file inspection and a draft schema).
- Public `Fetcher` workflow (`check` / `plan` / `download` / directory & zip
  export) and `Result`.
- Source adapters: `cmems`, `ecmwf_open_data`, `era5`, `skytruth`, `hozint`,
  `copernicus_dataspace`, and `eodyn` (archive interim mode with an API seam).
- Gridded `Assembler`: regrid onto a common grid and align/aggregate time, with
  xarray / NetCDF / Zarr / NumPy output.
- `collekt fetch`, `collekt doctor`, and `collekt config show` command-line
  interface, plus `run_doctor` / `DoctorCheck` diagnostics.

### Removed

- Vestigial `supported_sampling` source metadata; datasets now declare only their
  native `temporal_sampling`.
- Legacy `collekt query` CLI, `Collector`, `Query`, the `DataSource` base classes,
  and the old `datasources/` package (superseded by the adapter framework).

### Changed

- Source collection now validates explicit variable/depth selections for bundled
  catalogs and only allows request sampling at dataset cadence or coarser
  integer multiples.
- CMEMS diagnostics and planning use declared coverage metadata as an offline
  fallback, with rolling NRT coverage handled separately from archive products.
- Example notebooks and source documentation were refreshed for the bundled
  catalogs and native-resolution CMEMS inspection workflow.
