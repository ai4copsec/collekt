"""Analysis-ready assembly of downloaded collections.

`Assembler` builds gridded outputs (xarray / NetCDF / Zarr / NumPy) from a
`collekt.Result`. Feature (tabular) sources are written directly by their
adapters and are not assembled here.
"""

from collekt.assemble.gridded import Assembler

__all__ = ["Assembler"]
