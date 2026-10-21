"""Public fetch workflow API."""

from __future__ import annotations

import tempfile
from dataclasses import replace
from pathlib import Path

from collekt.core.batching import validate_batch_days
from collekt.core.collect import run_collection
from collekt.core.config import Config
from collekt.core.datasets import DatasetConfig
from collekt.core.doctor import DoctorCheck, run_doctor
from collekt.core.exporting import export_directory, export_zip
from collekt.core.request import Request
from collekt.core.result import Result
from collekt.sources.base import ProgressCallback, null_progress


def _config_with_output_root(config: Config, output_root: Path) -> Config:
    raw = dict(config.raw)
    raw_output = dict(raw.get("output", {}) or {})
    raw_output["root"] = str(output_root)
    raw["output"] = raw_output
    return replace(config, output=replace(config.output, root=output_root), raw=raw)


class Fetcher:
    """Fetch source-native data for a request.

    `Fetcher` keeps request, configuration, plan, download, and export state
    together. It downloads source-native files into a staging collection
    directory, then exports that directory or a zip archive for downstream use.
    Analysis-ready outputs are built separately from the returned `Result`.

    Example:
        ```python
        import collekt

        config = collekt.DatasetConfig(
            collekt.CMEMS("cmems_duacs_my", variables=["ugos", "vgos"]),
        )
        request = collekt.Request(region=collekt.Region.from_bbox((-6.0, 20.0, 35.0, 45.0)), start="2023-06-15")

        fetcher = collekt.Fetcher(request=request, config=config, output_dir="collections")
        result = fetcher.download()
        fetcher.export_zip("collection.zip")
        ```

    Args:
        request: Region, time window, and metadata.
        config: Selected datasets and provider parameters.
        output_dir: Root directory for staged downloads.
        conf_dir: Optional configuration directory that extends the bundled
            catalog with additional or overriding datasets. Only used when
            `config` is a `DatasetConfig`.
        strict: Override config strict mode. If true, warnings become a final
            `RuntimeError` after the manifest is written.
        progress: Optional progress callback.
        batch_days: Maximum UTC calendar days per retrieval group. A positive
            integer enables provider-aware batching while retaining daily files
            for daily sources. `None` keeps the source's existing behavior.
    """

    def __init__(
        self,
        request: Request,
        *,
        config: DatasetConfig | Config,
        output_dir: str | Path | None = None,
        conf_dir: str | Path | None = None,
        strict: bool | None = None,
        progress: ProgressCallback = null_progress,
        batch_days: int | None = None,
    ) -> None:
        validate_batch_days(batch_days)
        self.batch_days = batch_days
        self.request = request
        self.strict = strict
        self.progress = progress
        self.plan_result: Result | None = None
        self.download_result: Result | None = None
        self.doctor_checks: tuple[DoctorCheck, ...] = ()

        self.dataset_config = config
        loaded_config = config.resolve(conf_dir=conf_dir) if isinstance(config, DatasetConfig) else config
        if output_dir is not None:
            root = Path(output_dir).expanduser()
        elif isinstance(config, Config):
            root = loaded_config.output.root
        else:
            root = Path(tempfile.mkdtemp(prefix="collekt_"))
        self.config = _config_with_output_root(loaded_config, root)

    @property
    def output_dir(self) -> Path:
        """Root directory used for staged downloads."""
        return self.config.output.root

    def check(self, *, online: bool = False) -> Result:
        """Run diagnostics, then return a dry-run plan.

        Args:
            online: If true, query provider catalogues where supported.

        Returns:
            Planned collection result. Diagnostics are also stored in
            `doctor_checks`.
        """
        self.doctor_checks = tuple(run_doctor(online=online, config=self.config))
        return self.plan()

    def plan(self) -> Result:
        """Plan provider requests and output paths without downloading files.

        Returns:
            Planned collection result.
        """
        self.plan_result = run_collection(
            self.request,
            config=self.config,
            strict=self.strict,
            dry_run=True,
            batch_days=self.batch_days,
            progress=self.progress,
        )
        return self.plan_result

    def download(self, *, use_cache: bool = True) -> Result:
        """Download configured source-native files into the staging directory.

        The staging directory is keyed by region and time range, so cached files
        are only reused when both match the request.

        Args:
            use_cache: If true (default), reuse files already staged for this
                exact request. If false, delete the request directory and
                download everything again.

        Returns:
            Collection result with downloaded, reused, skipped, and failed
            source results.

        Raises:
            RuntimeError: If strict mode is enabled and any source was skipped
                or failed.
        """
        self.download_result = run_collection(
            self.request,
            config=self.config,
            strict=self.strict,
            use_cache=use_cache,
            dry_run=False,
            batch_days=self.batch_days,
            progress=self.progress,
        )
        return self.download_result

    def export_directory(self, output_dir: str | Path, *, overwrite: bool = False) -> Path:
        """Export the downloaded collection as a directory copy.

        Args:
            output_dir: Destination directory.
            overwrite: If true, replace an existing destination.

        Returns:
            The destination directory.

        Raises:
            RuntimeError: If `download` has not been called yet.
            FileExistsError: If the destination exists and overwrite is false.
        """
        return export_directory(self._downloaded_output_dir(), output_dir, overwrite=overwrite)

    def export_zip(self, output_path: str | Path, *, overwrite: bool = False) -> Path:
        """Export the downloaded collection as a zip archive.

        Args:
            output_path: Destination zip file.
            overwrite: If true, replace an existing zip file.

        Returns:
            The destination zip file.

        Raises:
            RuntimeError: If `download` has not been called yet.
            FileExistsError: If the destination exists and overwrite is false.
        """
        return export_zip(self._downloaded_output_dir(), output_path, overwrite=overwrite)

    def _downloaded_output_dir(self) -> Path:
        if self.download_result is None:
            raise RuntimeError("download() must be called before exporting")
        return self.download_result.output_dir
