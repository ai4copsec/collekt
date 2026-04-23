import datetime as dt
import logging
import tempfile
from pathlib import Path

from collekt.datasources.hozint import Hozint

logger = logging.getLogger(__name__)

class Collector:
    datasources = [
            Hozint
    ]

    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            log_level: str | None = None,
            verbose: str | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
            ) -> list[any]:
        logger.info(f"Starting collection - {output_dir=}")

        for datasource in self.datasources:
            logger.info(f"Querying {datasource=}")
            try:
                ds = datasource()
                ds.execute(
                    from_time=from_time,
                    to_time=to_time,
                    longitude=longitude,
                    latitude=latitude,
                    radius=radius,
                    region=region,
                    output_dir=output_dir,
                    log_level=log_level,
                    verbose=verbose
                )
            except Exception as e:
                logger.warning(f"Querying {datasource=} failed -- {e}")
