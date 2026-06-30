import datetime as dt
from pathlib import Path

from pydantic import BaseModel, Field


class Query(BaseModel):
    from_time: dt.datetime | None = Field(default=None, description="Min time to consider")
    to_time: dt.datetime | None = Field(default=None, description="Max time to consider")

    longitude: float | None = Field(default=None, description="Longitude in deg")
    latitude: float | None = Field(default=None, description="Latitude in deg")

    radius: float | None = Field(default=None, description="Radius in km")
    region: Path | None = Field(default=None, description="Path to geojson file describing the region")

