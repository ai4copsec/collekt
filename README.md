# collekt

A library to facilitate spatio-temporal data collection across arbitrary
datasources.

A single request — a region of interest, a time window, an optional sampling and
set of variable groups — is fanned out to a set of pluggable source adapters.
Each adapter fetches source-native files (NetCDF, GRIB2, Parquet, product
archives) and records what it did in a machine-readable manifest. Gridded results
can then be assembled onto a common grid and time axis.

## Installation

Requires Python 3.13+ and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/ai4copsec/collekt.git
cd collekt
uv sync --all-groups --all-extras   # or: just install
```

Heavy provider clients are optional; install only the extras you need
(`gridded`, `features`, `cmems`, `era5`, `ecmwf`, `hozint`, or `all`). The base
install stays light.

## Usage

### Python

```python
import collekt

request = collekt.Request(
    region=collekt.Region.from_bbox((-6.0, 20.0, 35.0, 45.0)),
    start="2023-06-15",
    variables=("currents", "wind"),
    sampling="6h",
)

fetcher = collekt.Fetcher(request, conf_dir="path/to/catalogs")
result = fetcher.download()
print(result.manifest_path)

# Assemble gridded sources onto a common grid and time axis.
assembler = collekt.Assembler(result)
dataset = assembler.to_xarray(grid="lowest_resolution", time="lowest_resolution")
```

`Region` can also be built from a point and radius
(`Region.from_point_radius(lat, lon, radius_km)`) or a GeoJSON file
(`Region.from_geojson(path)`).

### Command line

```bash
collekt fetch --bbox -6 20 35 45 --start 2023-06-15 --sampling 6h \
  --variables currents,wind --conf-dir path/to/catalogs

collekt fetch --at-lat 13.3 --at-lon 42.9 --radius 50 --start 2024-01-30 \
  --datasource skytruth --conf-dir path/to/catalogs --dry-run

collekt doctor            # check the environment and configuration
collekt config show       # print the merged configuration
```

Use `--dry-run` to plan provider requests without downloading, and `--strict` to
fail if any requested source is skipped.

### Sources and configuration

collekt ships the source **adapters** and the configuration **mechanism**. Which
datasets and variables each source pulls (the catalog), and any curated presets,
are supplied by the caller through a configuration directory (`--conf-dir` /
`conf_dir=`). See [Products](https://ai4copsec.github.io/collekt/products.html)
and [Credentials](https://ai4copsec.github.io/collekt/credentials.html).

## Development

```bash
just lint    # format + lint with ruff
just test    # run the test suite
just docs    # build the Quarto documentation
```

The definition of done for any change: `just lint` and `just test` pass, with a
test for any behavior changed.

## Documentation

Built with [Quarto](https://quarto.org) and
[quartodoc](https://machow.github.io/quartodoc/) from `docs/`; the rendered site
is published to GitHub Pages.

## License

[BSD-3-Clause](LICENSE).

## Copyright

Copyright (c) 2023-2026 [Simula Research Laboratory, Oslo, Norway](https://www.simula.no).

## Acknowledgments

Part of the EU project [AI4COPSEC](https://ai4copsec.eu), funded by the Horizon
Europe framework programme under Grant Agreement N. 101190021.
