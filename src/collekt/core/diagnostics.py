"""Shared diagnostic result types and helpers."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class DoctorStatus(StrEnum):
    """Diagnostic status values."""

    OK = "ok"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class DoctorCheck:
    """One diagnostic check result."""

    name: str
    status: DoctorStatus
    message: str

    def as_dict(self) -> dict[str, str]:
        """Return a JSON-serializable mapping."""
        return {"name": self.name, "status": str(self.status), "message": self.message}


def package_check(name: str, import_name: str) -> DoctorCheck:
    """Return whether a Python package can be imported."""
    try:
        spec = importlib.util.find_spec(import_name)
    except ModuleNotFoundError:
        spec = None
    if spec is None:
        return DoctorCheck(name=name, status=DoctorStatus.WARN, message=f"{import_name!r} is not installed")
    return DoctorCheck(name=name, status=DoctorStatus.OK, message=f"{import_name!r} is importable")


def path_exists(path: Path | None) -> bool:
    """Return whether a configured path exists."""
    return path is not None and path.expanduser().exists()
