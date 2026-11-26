"""Shared fixtures for the core test suite."""

from datetime import timedelta
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from collekt.sources import eodyn


class FakeDocuments:
    """Stand-in for `Oda().documents`, matching ODA's overlap date filter."""

    def __init__(self, documents):
        self.documents = documents
        self.queries = []
        self.downloads = []

    def find(self, *, dataset_name, date__gte=None, date__lte=None, order_by="+name"):
        self.queries.append(dataset_name)
        found = [
            document
            for document in self.documents
            if (date__gte is None or document.end_datetime >= date__gte)
            and (date__lte is None or document.start_datetime <= date__lte)
        ]
        return sorted(found, key=lambda document: document.start_datetime, reverse=order_by.startswith("-"))


def _osmose_file(path, time):
    """Write a small OSmose-like L4 file whose values encode its analysis hour."""
    lat = np.arange(35.0, 45.5, 1.0)
    lon = np.arange(-6.0, 20.5, 1.0)
    field = np.full((1, lat.size, lon.size), float(time.hour))
    xr.Dataset(
        {name: (("time", "latitude", "longitude"), field.copy()) for name in ("totalewct", "totalnsct", "ci")},
        coords={"time": [np.datetime64(time.replace(tzinfo=None), "ns")], "latitude": lat, "longitude": lon},
        attrs={"id": "EODYN-OS-VELOCITY-L4", "analysis_time": time.isoformat()},
    ).to_netcdf(path, encoding={"time": {"units": "days since 1900-01-01", "dtype": "float64"}})  # as ODA files


@pytest.fixture
def fake_oda(monkeypatch, tmp_path):
    """Serve synthetic OSmose L4 files through a fake ODA client.

    Call the fixture with the analysis times (aware UTC datetimes) to publish and
    an optional ``(west, east, south, north)`` footprint. It sets ODA credentials,
    installs the client, and returns its `FakeDocuments`, whose ``queries`` and
    ``downloads`` record what the adapter asked for.
    """

    def install(times, footprint=(-6.0, 20.0, 35.0, 45.0)):
        directory = tmp_path / "oda"
        directory.mkdir(exist_ok=True)
        west, east, south, north = footprint
        ring = [(west, south), (east, south), (east, north), (west, north), (west, south)]
        area = SimpleNamespace(coordinates=[[SimpleNamespace(lon=lon, lat=lat) for lon, lat in ring]])
        documents = FakeDocuments([])
        for time in times:
            name = f"OMNISITU_OSMOSE_L4_MED_{time:%Y-%m-%dT%H-%M-%S}-PT3H.nc"
            path = directory / name
            _osmose_file(path, time)

            def download(path=path, name=name, **_):
                documents.downloads.append(name)
                return SimpleNamespace(byte_stream=iter([path.read_bytes()]))

            documents.documents.append(
                SimpleNamespace(
                    name=name,
                    start_datetime=time,
                    end_datetime=time + timedelta(hours=3),
                    area_geojson=area,
                    download=download,
                )
            )
        monkeypatch.setenv("ODA_USERNAME", "user")
        monkeypatch.setenv("ODA_PASSWORD", "secret")
        monkeypatch.setattr(eodyn, "_oda_client", lambda: SimpleNamespace(documents=documents))
        return documents

    return install
