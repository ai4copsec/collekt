"""Public fetch workflow API."""

from __future__ import annotations

import tempfile
from dataclasses import replace
from pathlib import Path

from collekt.core.collect import run_collection
from collekt.core.config import Config, SourceVariableOverrides, apply_source_variable_overrides, get_config
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

        request = collekt.Request(
            region=collekt.Region.from_bbox((-6.0, 20.0, 35.0, 45.0)),
            start="2023-06-15",
            variables=("currents", "wind"),
            sampling="6h",
        )

        fetcher = collekt.Fetcher(request)
        result = fetcher.download()
        fetcher.export_zip("collection.zip")
        ```

    Args:
        request: Region, time window, requested variable groups, and sampling.
        preset: Optional configuration preset.
        config: Optional preloaded configuration. When provided, its output root
            is used as the staging root unless `staging_root` is set.
        conf_dir: Optional configuration directory.
        strict: Override config strict mode. If true, warnings become a final
            `RuntimeError` after the manifest is written.
        source_variable_overrides: Optional mapping from source name to
            ``use_variables`` values for targeted product-variable selection.
        use_datasources: Optional lower-cased source names to restrict the run to.
        staging_root: Optional root directory for staged downloads. By default a
            temporary directory is created when `config` is not supplied.
        progress: Optional progress callback.
    """

    def __init__(
        self,
        request: Request,
        *,
        preset: str | None = None,
        config: Config | None = None,
        conf_dir: str | Path | None = None,
        strict: bool | None = None,
        source_variable_overrides: SourceVariableOverrides | None = None,
        use_datasources: list[str] | None = None,
        staging_root: str | Path | None = None,
        progress: ProgressCallback = null_progress,
    ) -> None:
        self.request = request
        self.preset = preset
        self.conf_dir = conf_dir
        self.strict = strict
        self.use_datasources = use_datasources
        self.progress = progress
        self.plan_result: Result | None = None
        self.download_result: Result | None = None
        self.doctor_checks: tuple[DoctorCheck, ...] = ()

        loaded_config = config or get_config(preset=preset, conf_dir=conf_dir)
        if staging_root is None and config is None:
            staging_root = tempfile.mkdtemp(prefix="collekt_")
        root = Path(staging_root).expanduser() if staging_root is not None else loaded_config.output.root
        self.config = apply_source_variable_overrides(
            _config_with_output_root(loaded_config, root), source_variable_overrides
        )

    @property
    def staging_root(self) -> Path:
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
            preset=self.preset,
            config=self.config,
            conf_dir=self.conf_dir,
            strict=self.strict,
            use_datasources=self.use_datasources,
            dry_run=True,
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
            preset=self.preset,
            config=self.config,
            conf_dir=self.conf_dir,
            strict=self.strict,
            use_datasources=self.use_datasources,
            use_cache=use_cache,
            dry_run=False,
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
