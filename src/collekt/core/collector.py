import datetime as dt
import logging
import tempfile
import traceback as tb
from pathlib import Path

from collekt.datasources.copernicus import CopernicusDataspace
from collekt.datasources.copernicusmarine import CopernicusMarineDataset
from collekt.datasources.hozint import Hozint
from collekt.datasources.skytruth import SkytruthDataset

logger = logging.getLogger(__name__)

class Collector:
    datasources = [
            Hozint(),
            CopernicusMarineDataset(
                dataset_id="cmems_obs-sst_glo_phy_l3s_gir_P1D-m"
            ),
            CopernicusDataspace(collection="sentinel-1-grd"),
            SkytruthDataset()
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
            output_dir: Path | None = Path(tempfile.gettempdir()),
            use_datasources: list[str] | None = None
            ) -> list[any]:
        logger.info(f"Starting collection - {output_dir=}")

        for datasource in self.datasources:
            if use_datasources and datasource.name.lower() not in use_datasources:
                continue

            logger.info(f"Querying {datasource=}")
            try:
                datasource.execute(
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
                if verbose:
                    tb.print_exception(e)

                logger.warning(f"Querying {datasource=} failed -- {e}")
