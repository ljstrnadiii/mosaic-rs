from __future__ import annotations

from enum import Enum
from functools import wraps
from types import TracebackType
from typing import Any

from ._mosaic_rs import (
    BBox,
    MosaicSpec,
    OutputWindow,
    Raster,
    TileRecord,
    ___version,
    build_mosaic as _build_mosaic,
    build_mosaic_async as _build_mosaic_async,
    flush_tracing as _flush_tracing,
    init_tracing as _init_tracing,
    shutdown_tracing as _shutdown_tracing,
)

__version__: str = ___version()


class Resampling(str, Enum):
    NEAREST = "Nearest"
    BILINEAR = "Bilinear"
    CUBIC = "Cubic"
    AVERAGE = "Average"
    SUM = "Sum"


@wraps(_build_mosaic)
def build_mosaic(*args: Any, **kwargs: Any) -> Raster:
    """Build a mosaic synchronously.

    Parameters
    ----------
    spec : MosaicSpec
        Target output grid and raster configuration.
    tiles : list[TileRecord]
        Source tiles to compose into the mosaic.
    store : ObjectStore
        Object store used to fetch source tile bytes.
    max_tile_concurrency : int, default=32
        Maximum number of tile/block tasks processed concurrently.
    max_work_concurrency : int, default=16
        Maximum number of CPU-bound warp/reproject tasks run concurrently.
    cache_meta_max_bytes : int | None, default=None
        Optional metadata cache size budget in bytes.
    cache_pixel_max_bytes : int | None, default=None
        Optional pixel cache size budget in bytes.
    z_limit : int | None, default=None
        Optional limit for z-ordered tile composition.
    working_type : type[np.float32] | type[np.float64] | None, default=None
        Working precision for reprojection. ``None`` maps to auto precision.

    Returns
    -------
    Raster
        Output raster with shape ``(height, width, bands)``.

    Raises
    ------
    RuntimeError
        Raised when mosaic build fails in the Rust core.
    ValueError
        Raised when options are invalid.
    """
    return _build_mosaic(*args, **kwargs)


@wraps(_build_mosaic_async)
def build_mosaic_async(*args: Any, **kwargs: Any) -> Any:
    """Build a mosaic asynchronously.

    Parameters
    ----------
    spec : MosaicSpec
        Target output grid and raster configuration.
    tiles : list[TileRecord]
        Source tiles to compose into the mosaic.
    store : ObjectStore
        Object store used to fetch source tile bytes.
    max_tile_concurrency : int, default=32
        Maximum number of tile/block tasks processed concurrently.
    max_work_concurrency : int, default=16
        Maximum number of CPU-bound warp/reproject tasks run concurrently.
    cache_meta_max_bytes : int | None, default=None
        Optional metadata cache size budget in bytes.
    cache_pixel_max_bytes : int | None, default=None
        Optional pixel cache size budget in bytes.
    z_limit : int | None, default=None
        Optional limit for z-ordered tile composition.
    working_type : type[np.float32] | type[np.float64] | None, default=None
        Working precision for reprojection. ``None`` maps to auto precision.

    Returns
    -------
    Awaitable[Raster]
        Awaitable that resolves to the output raster.

    Raises
    ------
    RuntimeError
        Raised when mosaic build fails in the Rust core.
    ValueError
        Raised when options are invalid.
    """
    return _build_mosaic_async(*args, **kwargs)


@wraps(_init_tracing)
def init_tracing(*args: Any, **kwargs: Any) -> bool:
    """Initialize Rust tracing for the current Python process.

    Parameters
    ----------
    rust_log : str | None, default=None
        Rust log filter (for example ``"mosaic=trace,mosaic_rs=trace"``).
    perfetto_path : str | None, default=None
        Output file path for Perfetto-compatible JSON trace events.
    include_args : bool, default=False
        Whether to include span arguments in trace events.

    Returns
    -------
    bool
        ``True`` if this call performed initialization, ``False`` if tracing
        was already initialized.

    Raises
    ------
    RuntimeError
        Raised when tracing cannot be initialized with the requested settings.
    """
    return _init_tracing(*args, **kwargs)


@wraps(_flush_tracing)
def flush_tracing() -> bool:
    """Flush pending tracing data.

    Returns
    -------
    bool
        ``True`` on success.
    """
    return _flush_tracing()


@wraps(_shutdown_tracing)
def shutdown_tracing() -> bool:
    """Shutdown tracing and close active Perfetto trace output.

    Returns
    -------
    bool
        ``True`` on success.
    """
    return _shutdown_tracing()


class TracingSession:
    def __init__(
        self,
        *,
        rust_log: str | None = None,
        perfetto_path: str | None = None,
        include_args: bool = False,
    ) -> None:
        self.rust_log = rust_log
        self.perfetto_path = perfetto_path
        self.include_args = include_args
        self._owns_init = False

    def __enter__(self) -> "TracingSession":
        self._owns_init = init_tracing(
            rust_log=self.rust_log,
            perfetto_path=self.perfetto_path,
            include_args=self.include_args,
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if self._owns_init:
            flush_tracing()
            shutdown_tracing()
        return False

__all__ = [
    "BBox",
    "MosaicSpec",
    "OutputWindow",
    "Raster",
    "Resampling",
    "TileRecord",
    "build_mosaic",
    "build_mosaic_async",
    "TracingSession",
    "init_tracing",
    "flush_tracing",
    "shutdown_tracing",
]
