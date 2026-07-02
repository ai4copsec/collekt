"""Export helpers for downloaded collekt collections."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path


def export_directory(source_dir: Path, output_dir: str | Path, *, overwrite: bool = False) -> Path:
    """Copy a downloaded collection directory to a destination.

    Args:
        source_dir: Collection directory produced by `Fetcher.download`.
        output_dir: Destination directory.
        overwrite: If true, replace an existing destination.

    Returns:
        The destination directory.

    Raises:
        FileExistsError: If the destination exists and overwrite is false.
        FileNotFoundError: If the source directory does not exist.
    """
    source = Path(source_dir)
    destination = Path(output_dir)
    if not source.exists():
        raise FileNotFoundError(f"collection directory does not exist: {source}")
    if destination.exists():
        if not overwrite:
            raise FileExistsError(f"export destination already exists: {destination}")
        if destination.is_dir():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination)
    return destination


def export_zip(source_dir: Path, output_path: str | Path, *, overwrite: bool = False) -> Path:
    """Archive a downloaded collection directory as a zip file.

    Args:
        source_dir: Collection directory produced by `Fetcher.download`.
        output_path: Destination zip file.
        overwrite: If true, replace an existing zip file.

    Returns:
        The destination zip file.

    Raises:
        FileExistsError: If the destination exists and overwrite is false.
        FileNotFoundError: If the source directory does not exist.
    """
    source = Path(source_dir)
    destination = Path(output_path)
    if not source.exists():
        raise FileNotFoundError(f"collection directory does not exist: {source}")
    if destination.exists():
        if not overwrite:
            raise FileExistsError(f"export destination already exists: {destination}")
        if destination.is_dir():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(source))
    return destination
