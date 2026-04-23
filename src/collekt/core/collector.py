import logging
import datetime as dt
from pathlib import Path

import tempfile

logger = logging.getLogger(__name__)

class Collector:
    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
            ) -> list[any]:
        logger.info(f"Starting collection - {output_dir=}")

