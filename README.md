# collekt

A library to facilitate spatio-temporal data collection across arbitrary
datasources.

A dataset config names the products to fetch, and a request supplies the region,
time window, and metadata. The selected products are fanned out to
pluggable source adapters.
Each adapter fetches source-native files (NetCDF, GRIB2, Parquet, product
archives) and records what it did in a machine-readable manifest. Gridded results
can then be assembled onto a common grid and time axis.

## Installation

Requires Python 3.13+ and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/ai4copsec/collekt.git
cd collekt
uv sync --all-groups  # or: just install
```

All provider clients and the gridded `Assembler` are installed by default, so
every source works out of the box. Clients are imported lazily, so importing
collekt stays fast.

To build the documentation locally, you will also need the `quarto` binary ([quarto.org](https://quarto.org/)):

```bash
just docs
# or :
uv run quartodoc build --config docs/_quarto.yml
quarto render docs
```

We recommend using `just` (<https://github.com/casey/just>) with the make-like commands available in `justfile`.

## Usage

### Python

```python
import collekt

config = collekt.DatasetConfig(
    collekt.CMEMS("cmems_glorys_my", variables=["uo", "vo"], depth=[1.0, 1.1]),
    collekt.CMEMS("cmems_duacs_my", variables=["ugos", "vgos"]),
    collekt.CMEMS("cmems_med_currents_my", variables=["uo", "vo"], depth=[1.0, 1.1]),
)
request = collekt.Request(
    region=collekt.Region.from_bbox((-6.0, 20.0, 35.0, 45.0)),
    start="2023-06-15",
)

fetcher = collekt.Fetcher(request=request, config=config, output_dir="data/collections")
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
collekt config show       # inspect the bundled catalog
collekt doctor            # check the environment and configuration

cat > datasets.yaml <<'YAML'
datasets:
  - provider: cmems
    key: cmems_glorys_my
    variables: [uo, vo]
    depth: [1.0, 1.1]
  - provider: cmems
    key: cmems_duacs_my
    variables: [ugos, vgos]
  - provider: cmems
    key: cmems_med_currents_my
    variables: [uo, vo]
    depth: [1.0, 1.1]
YAML

collekt fetch --bbox -6 20 35 45 --start 2023-06-15 \
  --dataset-config datasets.yaml --output-dir data/collections
```

Use `--dry-run` to plan provider requests without downloading, and `--strict` to
fail if any requested source is skipped.

### Sources and configuration

collekt ships a **curated catalog** of datasets (CMEMS global + Mediterranean,
ECMWF Open Data, ERA5, Skytruth, Copernicus Data Space, eOdyn). Select concrete
dataset keys with `DatasetConfig` or its YAML form, and put provider parameters
such as variable names and depth ranges directly on each selected dataset. See
[Products](https://ai4copsec.github.io/collekt/products.html) and
[Credentials](https://ai4copsec.github.io/collekt/credentials.html).

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

*collekt* does not grant you any rights to the wrapped third party providers - it only simplifies making multiple-queries to the sources you already have the permission to access.

Each wrapped service comes with its own separate "Terms of Use", e.g., [Global Fishing Watch](https://globalfishingwatch.org/terms-of-use/) is for non-commercial use only - others my carry similar restrictions.

## Copyright

Copyright (c) 2023-2026 [Simula Research Laboratory, Oslo, Norway](https://www.simula.no).

## Acknowledgments

Part of the EU project [AI4COPSEC](https://ai4copsec.eu), funded by the Horizon
Europe framework programme under Grant Agreement N. 101190021.
