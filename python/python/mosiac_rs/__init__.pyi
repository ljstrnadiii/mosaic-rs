from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Awaitable

import numpy as np
import numpy.typing as npt
from obstore.store import ObjectStore

__version__: str

class Resampling(str, Enum):
    NEAREST = "Nearest"
    BILINEAR = "Bilinear"
    CUBIC = "Cubic"
    AVERAGE = "Average"
    SUM = "Sum"

class BBox:
    def __init__(self, minx: float, miny: float, maxx: float, maxy: float) -> None: ...
    @property
    def minx(self) -> float: ...
    @property
    def miny(self) -> float: ...
    @property
    def maxx(self) -> float: ...
    @property
    def maxy(self) -> float: ...

class OutputWindow:
    def __init__(self, x_off: int, y_off: int, width: int, height: int) -> None: ...
    @property
    def x_off(self) -> int: ...
    @property
    def y_off(self) -> int: ...
    @property
    def width(self) -> int: ...
    @property
    def height(self) -> int: ...

class MosaicSpec:
    def __init__(
        self,
        resx: float,
        resy: float,
        bbox: BBox,
        dst_crs: str,
        *,
        band_count: int = 1,
        dtype: np.dtype[Any] | None = None,
        blockxsize: int = 1024,
        blockysize: int = 1024,
        resampling: Resampling = Resampling.NEAREST,
        sort_ascending: bool = True,
        output_nodata: float = -9999.0,
        window: OutputWindow | None = None,
    ) -> None: ...

class TileRecord:
    def __init__(
        self,
        location: str,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
        *,
        sort_key: int | float | str | datetime | None = None,
    ) -> None: ...
    @property
    def location(self) -> str: ...

class Raster:
    @property
    def width(self) -> int: ...
    @property
    def height(self) -> int: ...
    @property
    def bands(self) -> int: ...
    @property
    def shape(self) -> tuple[int, int, int]: ...
    def data(self) -> npt.NDArray[np.float32]: ...

class TracingSession:
    def __init__(
        self,
        *,
        rust_log: str | None = None,
        perfetto_path: str | None = None,
        include_args: bool = False,
    ) -> None: ...
    def __enter__(self) -> TracingSession: ...
    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool: ...

def build_mosaic(
    spec: MosaicSpec,
    tiles: list[TileRecord],
    *,
    store: ObjectStore,
    max_tile_concurrency: int,
    max_work_concurrency: int,
    cache_meta_max_bytes: int | None = None,
    cache_pixel_max_bytes: int | None = None,
    z_limit: int | None = None,
    working_type: type[np.float32] | type[np.float64] | None = None,
) -> Raster: ...
def build_mosaic_async(
    spec: MosaicSpec,
    tiles: list[TileRecord],
    *,
    store: ObjectStore,
    max_tile_concurrency: int = 32,
    max_work_concurrency: int = 16,
    cache_meta_max_bytes: int | None = None,
    cache_pixel_max_bytes: int | None = None,
    z_limit: int | None = None,
    working_type: type[np.float32] | type[np.float64] | None = None,
) -> Awaitable[Raster]: ...
def init_tracing(
    *,
    rust_log: str | None = None,
    perfetto_path: str | None = None,
    include_args: bool = False,
) -> bool: ...
def flush_tracing() -> bool: ...
def shutdown_tracing() -> bool: ...

__all__: list[str]
