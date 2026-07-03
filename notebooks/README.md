# Notebooks

Runnable demos for collekt. Install the dependencies (including the notebook
group), then launch Jupyter:

```bash
just install         # includes the notebook group (jupyterlab, matplotlib)
uv run jupyter lab   # then open a notebook
```

Downloads and assembled outputs are written under `notebooks/cache/`, which is
git-ignored. The first run downloads; later runs reuse the cache.

- **01_westmed_cmems.ipynb** — collect CMEMS surface currents (GLORYS, DUACS,
  MEDFS) over the western Mediterranean for 2023-06-15, then assemble them onto a
  common grid. Requires Copernicus Marine credentials (`copernicusmarine login`).
- **02_westmed_skytruth.ipynb** — query Skytruth oil-slick detections over the
  western Mediterranean for June 2023. Open source — no credentials needed.
