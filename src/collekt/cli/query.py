import datetime as dt
import logging
import tempfile
from argparse import ArgumentParser
from pathlib import Path

from collekt.cli.base import BaseParser
from collekt.core.collector import Collector

logger = logging.getLogger(__name__)

class QueryParser(BaseParser):
    """
    :param parser: The base parser
    """

    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.description = "collekt query"

        parser.add_argument("--from-time", type=str, help="Starting date in isoformat")
        parser.add_argument("--to-time", type=str, help="End date in isoformat")

        parser.add_argument("--at-lat", type=float, default=None, help="At latitude")
        parser.add_argument("--at-lon", type=float, default=None, help="At longitude")
        parser.add_argument("--radius", type=float, default=None, help="Radius in km")

        parser.add_argument("--region", type=Path, default=None, help="A geojson file describing a region")

        default_output_dir = Path(tempfile.gettempdir()) / "collekt" / f"{dt.datetime.now(tz=dt.timezone.utc).strftime('%Y%m%d-%H:%M:%S+00:00')}"
        parser.add_argument("--output-dir", type=str, default=str(default_output_dir), help="Output directory to store the data, default: %(default)s")

        datasource_choices = [x.name.lower() for x in Collector.datasources]
        parser.add_argument("--datasource",
                            nargs="+",
                            choices=datasource_choices,
                            metavar='DATASOURCE',
                            default=None,
                            help=f"Select datasource from: {','.join(datasource_choices)}"
        )



    def execute(self, args):
        super().execute(args)

        from_time = None
        if args.from_time:
            from_time = dt.datetime.fromisoformat(args.from_time)

        to_time = None
        if args.to_time:
            to_time = dt.datetime.fromisoformat(args.to_time)

        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        collector = Collector()
        collector.execute(from_time=from_time,
                          to_time=to_time,
                          longitude=args.at_lon,
                          latitude=args.at_lat,
                          radius=args.radius,
                          region=args.region,
                          output_dir=output_dir,
                          log_level=args.log_level,
                          verbose=args.verbose,
                          use_datasources=args.datasource
                )
