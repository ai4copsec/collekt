import datetime as dt
import subprocess
import tempfile
from pathlib import Path


class DataSource:
    name: str

    def __init__(self, name: str):
        self.name = name

    def execute(self,
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
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
            from_time: dt.datetime | None = None,
            to_time: dt.datetime | None = None,
            longitude: float | None = None,
            latitude: float | None = None,
            radius: float | None = None,
            region: Path | None = None,
            log_level: str | None = None,
            verbose: bool | None = None,
            output_dir: Path | None = Path(tempfile.gettempdir())
            ) -> list[any]:

        cmd = self.command.copy()

        if log_level:
            cmd += [ "--log-level", log_level ]

        if verbose:
            cmd += [ "--verbose"]

        if from_time:
            cmd += [ "--from-time", from_time.strftime(self.time_format)]

        if to_time:
            cmd += [ "--to-time", to_time.strftime(self.time_format)]

        if latitude:
            cmd += [ "--at-lat", latitude ]

        if longitude:
            cmd += [ "--at-lon", latitude ]

        if radius:
            cmd += [ "--radius", radius ]

        if region:
            cmd += [ "--region", region ]

        if output_dir:
            cmd += [ "--output-dir", output_dir ]

        subprocess.run(cmd)
