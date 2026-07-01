import subprocess
import tempfile
from pathlib import Path

from .types import Query


class DataSource:
    name: str

    def __init__(self, name: str):
        self.name = name

    def execute(self,
            query: Query = Query(),
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
           ) -> list[any]:
        raise NotImplementedError(f"DataSource.execute: not implemented for {self.name}")


class CLIDataSource(DataSource):
    command: str
    time_format: str

    def __init__(self,
                 name: str,
                 cmd: list[str],
                 time_format: str = '%Y%m%d-%H%M'
            ):
        super().__init__(name=name)

        self.command = cmd
        self.time_format = time_format

    def execute(self,
            query: Query = Query(),
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
            ) -> list[any]:

        cmd = self.command.copy()

        if log_level:
            cmd += [ "--log-level", log_level ]

        if verbose:
            cmd += [ "--verbose"]

        if output_dir:
            cmd += [ "--output-dir", output_dir ]

        # Query
        if query.from_time:
            cmd += [ "--from-time", query.from_time.strftime(self.time_format)]

        if query.to_time:
            cmd += [ "--to-time", query.to_time.strftime(self.time_format)]

        if query.latitude is not None:
            cmd += [ "--at-lat", str(query.latitude) ]

        if query.longitude is not None:
            cmd += [ "--at-lon", str(query.latitude) ]

        if query.radius is not None:
            cmd += [ "--radius", str(query.radius) ]

        if query.region is not None:
            cmd += [ "--region", query.region ]

        subprocess.run(cmd)
