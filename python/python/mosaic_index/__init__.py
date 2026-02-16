from __future__ import annotations

from types import TracebackType

from ._mosaic_index import (
    BBox,
    MosaicSpec,
    OutputWindow,
    Raster,
    TileRecord,
    ___version,
    build_mosaic,
    build_mosaic_async,
    flush_tracing,
    init_tracing,
    shutdown_tracing,
)

__version__: str = ___version()


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
    "TileRecord",
    "build_mosaic",
    "build_mosaic_async",
    "TracingSession",
    "init_tracing",
    "flush_tracing",
    "shutdown_tracing",
]
