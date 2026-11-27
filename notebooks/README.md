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
- **04_westmed_eodyn.ipynb** — fetch one day of eOdyn OSmose L4 surface
  currents from ODA (yesterday, or the latest published day) and plot how they
  evolve across its eight 3-hourly analyses: speed and direction, departure
  from the daily mean, the confidence index (`ci`), and an animation. Requires
  `ODA_USERNAME` / `ODA_PASSWORD`.
