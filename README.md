# mosaic-index

`mosaic-index` is a Rust crate for building output mosaics from an indexed set of source GeoTIFF/COG 
tiles with inspiration taken from GDAL's GTI driver.

It combines:
- tile indexing and ordering,
- lazy `async-tiff` reads,
- `warp-rs` reprojection,
- block-wise compositing,
- optional caching and perf/debug instrumentation.

**Note**: This is a project used to both learn more about GTI driver and rust is not intended to be
a production grade product.

## Core API

The main entrypoint in `src/lib.rs` is:

```rust
pub fn build_mosaic(
    spec: &MosaicSpec,
    tiles: impl IntoIterator<Item = TileRecord>,
    opts: BuildOptions,
) -> Result<RasterOwned>
```

There is also an async form:

```rust
pub async fn build_mosaic_async(
    spec: &MosaicSpec,
    tiles: impl IntoIterator<Item = TileRecord>,
    opts: BuildOptions,
) -> Result<RasterOwned>
```

## Quick usage

```rust
use std::sync::Arc;

use mosaic_index::{
    build_mosaic, BBox, BuildOptions, CacheConfig, DataType, MosaicSpec, Resample,
};

// tiles: Vec<TileRecord> from your index source.
# let tiles = Vec::new();
# let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

let spec = MosaicSpec {
    resx: 10.0,
    resy: 10.0,
    bbox: BBox::new(-105.2, 39.5, -104.7, 39.9),
    dst_crs: "EPSG:4326".to_string(),
    band_count: 5,
    data_type: DataType::F32,
    blockxsize: 1024,
    blockysize: 1024,
    resampling: Resample::Nearest,
    sort_ascending: true,
    output_nodata: -9999.0,
    window: None,
};

let opts = BuildOptions {
    tokio_handle: None,
    object_store: store,
    max_tile_concurrency: 32,
    max_work_concurrency: 16,
    cache: Some(CacheConfig {
        meta_max_bytes: 512 * 1024 * 1024,
        pixel_max_bytes: 2 * 1024 * 1024 * 1024,
    }),
    z_limit: Some(4),
    fetch_tiles_debug_log: None,
    perf_stats: None,
};

let raster = build_mosaic(&spec, tiles, opts)?;
# Ok::<(), mosaic_index::GtiError>(())
```

## Build options and observability

`BuildOptions` supports:
- `cache`: metadata + decoded tile cache byte budgets.
- `max_tile_concurrency`: block-level scheduling concurrency.
- `max_work_concurrency`: CPU-side decode/reproject concurrency.
- `fetch_tiles_debug_log`: captures every `async_tiff::fetch_tiles` call input.
- `perf_stats`: aggregate fetch/decode/reproject counters and timing.

## Notes

- Output type is `warp_rs::RasterOwned`.
- Source reads are windowed; only needed TIFF tiles are fetched.
- Pixel cache stores decoded source tiles keyed by `(uri, tile_x, tile_y)` to avoid repeated decode work across overlapping windows.


## TODO
1. drop z_limit
2. 