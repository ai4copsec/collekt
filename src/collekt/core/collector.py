import logging
import tempfile
import traceback as tb
from pathlib import Path

from collekt.core.types import Query
from collekt.datasources.copernicus import CopernicusDataspace
from collekt.datasources.copernicusmarine import CopernicusMarineDataset
from collekt.datasources.hozint import Hozint
from collekt.datasources.skytruth import SkytruthDataset

logger = logging.getLogger(__name__)


class Collector:
    datasources = [
        Hozint(),
        CopernicusMarineDataset(dataset_id="cmems_obs-sst_glo_phy_l3s_gir_P1D-m"),
        CopernicusDataspace(collection="sentinel-1-grd"),
        # Landsat 8 and Landsat 9 carry two distinct instruments: the OLI (Operational Land Imager) and the TIRS (Thermal Infrared Sensor).
        # Use Level-2 Science Products (landsat-c2-l2), which offer ready-to-use surface reflectance and temperature.
        CopernicusDataspace(collection="landsat-c2-l1-oli-tirs"),
        SkytruthDataset(),
    ]

    def execute(
        self,
        query: Query,
        log_level: str | None = None,
        verbose: str | None = None,
        output_dir: Path | None = Path(tempfile.gettempdir()),
        use_datasources: list[str] | None = None,
    ) -> list[any]:
        logger.info(f"Starting collection - {output_dir=}")

        for datasource in self.datasources:
            if use_datasources and datasource.name.lower() not in use_datasources:
                continue

            logger.info(f"Querying {datasource=}")
            try:
                datasource.execute(query=query, output_dir=output_dir, log_level=log_level, verbose=verbose)
            except Exception as e:
                if verbose:
                    tb.print_exception(e)

                logger.warning(f"Querying {datasource=} failed -- {e}")
