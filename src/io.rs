use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_tiff::decoder::DecoderRegistry;
use async_tiff::metadata::TiffMetadataReader;
use async_tiff::metadata::cache::ReadaheadMetadataCache;
use async_tiff::reader::{AsyncFileReader, ObjectReader};
use async_tiff::{CompressedBytes, ImageFileDirectory, Tile};
use object_store::ObjectStore;
use object_store::path::Path;
use rayon::prelude::*;
use tokio::sync::{Mutex, Semaphore};
use tracing::Instrument;
use url::Url;
use warp_rs::{GridSpec, PixelWindow, RasterLayout, RasterOwned, SourceTileLayout, SourceTiling};

use crate::cache::ByteLruCache;
use crate::types::{
    DebugWindow, FetchTilesDebugCall, FetchTilesDebugLog, GtiError, PerfStats, PerfStatsSink,
    Result, TileMeta,
};

pub struct TileHandle {
    pub uri: String,
    pub ifd: ImageFileDirectory,
    pub reader: Arc<dyn AsyncFileReader>,
    pub layout: RasterLayout,
    pub nodata: Option<f32>,
    pub src_grid: GridSpec,
    pub bands: usize,
}

/// Open a tile lazily using a caller-provided `ObjectStore`.
#[tracing::instrument(name = "tile.open", skip(store), fields(uri = %uri))]
pub async fn open_tile(uri: &str, store: Arc<dyn ObjectStore>) -> Result<TileHandle> {
    tracing::info!(target: "mosaic", uri, "open_tile: start");
    let url = Url::parse(uri).map_err(|e| GtiError::IndexLoad(format!("bad url {uri}: {e}")))?;
    let key = url.path().trim_start_matches('/').to_string();
    let path = Path::parse(&key)
        .map_err(|e| GtiError::IndexLoad(format!("bad object path {key}: {e}")))?;
    let reader: Arc<dyn AsyncFileReader> = Arc::new(ObjectReader::new(store, path));

    // Metadata with small readahead.
    let cache = ReadaheadMetadataCache::new(reader.clone());
    let mut meta = TiffMetadataReader::try_open(&cache)
        .instrument(tracing::debug_span!("async_tiff.read_metadata"))
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
    let ifd = meta
        .read_next_ifd(&cache)
        .instrument(tracing::debug_span!("async_tiff.read_next_ifd"))
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?
        .ok_or_else(|| GtiError::IndexLoad("no IFDs in TIFF".into()))?;

    let (affine, crs) = build_affine_and_crs(&ifd)?;
    let bands = ifd.samples_per_pixel() as usize;
    let width = ifd.image_width() as usize;
    let height = ifd.image_height() as usize;
    let src_grid = GridSpec::new(width, height, affine).with_crs(crs.clone());
    let nodata = ifd.gdal_nodata().and_then(|s| s.parse::<f32>().ok());

    let layout = match ifd.planar_configuration() {
        async_tiff::tags::PlanarConfiguration::Chunky => RasterLayout::Chunky,
        async_tiff::tags::PlanarConfiguration::Planar => RasterLayout::Planar,
        _ => RasterLayout::Chunky,
    };

    Ok(TileHandle {
        uri: uri.to_string(),
        ifd,
        reader,
        layout,
        nodata,
        src_grid,
        bands,
    })
}

pub fn tile_meta_from_handle(handle: &TileHandle, dst_crs: &str) -> Result<TileMeta> {
    let dst_to_src: std::sync::Arc<dyn warp_rs::CoordinateTransform> =
        if let Some(crs) = handle.src_grid.crs.clone() {
            if cfg!(feature = "proj") {
                std::sync::Arc::new(warp_rs::ProjTransform::new_known_crs(dst_crs, &crs)?)
            } else {
                std::sync::Arc::new(warp_rs::IdentityTransform)
            }
        } else {
            return Err(GtiError::InvalidSpec("tile missing CRS".into()));
        };

    Ok(TileMeta {
        bands: handle.bands,
        src_grid: handle.src_grid.clone(),
        dst_to_src,
        nodata: handle.nodata,
    })
}

/// Read just the requested pixel window into a RasterOwned.
pub type PixelCache = Arc<Mutex<ByteLruCache<SourceTileKey, Arc<async_tiff::Array>>>>;
pub type TileFetchGuards = Arc<Mutex<HashMap<SourceTileKey, Arc<Mutex<()>>>>>;

enum WindowTileInput {
    Decoded {
        tx: usize,
        ty: usize,
        arr: Arc<async_tiff::Array>,
    },
    Encoded(Tile),
}

#[tracing::instrument(
    name = "read_window_raster_f32",
    skip(
        handle,
        pixel_cache,
        tile_fetch_guards,
        fetch_tiles_debug_log,
        perf_stats
    ),
    fields(uri = uri, src_x = window.x, src_y = window.y, src_w = window.width, src_h = window.height)
)]
pub async fn read_window_raster_f32(
    handle: &TileHandle,
    window: PixelWindow,
    uri: &str,
    pixel_cache: Option<&PixelCache>,
    tile_fetch_guards: Option<&TileFetchGuards>,
    fetch_tiles_debug_log: Option<&FetchTilesDebugLog>,
    perf_stats: Option<&PerfStatsSink>,
    cpu_sem: &Arc<Semaphore>,
) -> Result<(RasterOwned, GridSpec)> {
    let width = handle.ifd.image_width() as usize;
    let height = handle.ifd.image_height() as usize;
    let bands = handle.ifd.samples_per_pixel() as usize;
    let tiling = SourceTiling::from_tiff_like(
        width,
        height,
        handle.ifd.tile_width().map(|v| v as usize),
        handle.ifd.tile_height().map(|v| v as usize),
        handle.ifd.rows_per_strip().map(|v| v as usize),
        match handle.layout {
            RasterLayout::Chunky => SourceTileLayout::Chunky,
            RasterLayout::Planar => SourceTileLayout::Planar,
        },
    );

    let tile_w = tiling.tile_width;
    let tile_h = tiling.tile_height;
    let tx0 = window.x / tile_w;
    let ty0 = window.y / tile_h;
    let tx1 = (window.x_end() - 1) / tile_w;
    let ty1 = (window.y_end() - 1) / tile_h;

    let mut xy = Vec::with_capacity((tx1 - tx0 + 1) * (ty1 - ty0 + 1));
    for ty in ty0..=ty1 {
        for tx in tx0..=tx1 {
            xy.push((tx, ty));
        }
    }
    let debug_window = DebugWindow {
        x: window.x,
        y: window.y,
        width: window.width,
        height: window.height,
    };

    let (tiles, held_key_guards) = if let Some(cache) = pixel_cache {
        let mut tiles: Vec<WindowTileInput> = Vec::with_capacity(xy.len());
        let mut misses = Vec::new();
        {
            let mut guard = cache.lock().await;
            for &(tx, ty) in &xy {
                let key = SourceTileKey::new(uri, tx, ty);
                if let Some(arr) = guard.get_cloned(&key) {
                    tiles.push(WindowTileInput::Decoded { tx, ty, arr });
                } else {
                    misses.push((tx, ty));
                }
            }
        }

        tracing::debug!(
            target: "mosaic",
            uri = uri,
            src_x = window.x,
            src_y = window.y,
            src_w = window.width,
            src_h = window.height,
            tile_count = xy.len(),
            tile_hits = tiles.len(),
            tile_misses = misses.len(),
            "decoded tile cache lookup"
        );

        let mut held_key_guards = None;
        if !misses.is_empty() {
            let mut still_missing = misses.clone();

            if let Some(fetch_guards) = tile_fetch_guards {
                let mut miss_keys = misses
                    .iter()
                    .map(|(tx, ty)| SourceTileKey::new(uri, *tx, *ty))
                    .collect::<Vec<_>>();
                miss_keys.sort();
                miss_keys.dedup();

                let key_guards = {
                    let mut guard_map = fetch_guards.lock().await;
                    miss_keys
                        .iter()
                        .map(|key| {
                            guard_map
                                .entry(key.clone())
                                .or_insert_with(|| Arc::new(Mutex::new(())))
                                .clone()
                        })
                        .collect::<Vec<_>>()
                };
                let mut local_key_guards = Vec::with_capacity(key_guards.len());
                for guard in key_guards {
                    local_key_guards.push(guard.lock_owned().await);
                }
                held_key_guards = Some(local_key_guards);

                still_missing.clear();
                let mut cache_guard = cache.lock().await;
                for &(tx, ty) in &misses {
                    let key = SourceTileKey::new(uri, tx, ty);
                    if let Some(arr) = cache_guard.get_cloned(&key) {
                        tiles.push(WindowTileInput::Decoded { tx, ty, arr });
                    } else {
                        still_missing.push((tx, ty));
                    }
                }
            }

            if !still_missing.is_empty() {
                let fetched_inputs = still_missing.clone();
                let fetched_tiles = fetch_tiles_timed(handle, &still_missing, perf_stats).await?;
                record_fetch_tiles_call(
                    fetch_tiles_debug_log,
                    FetchTilesDebugCall {
                        uri: uri.to_string(),
                        window: debug_window,
                        requested_tiles: xy.clone(),
                        fetched_tiles: fetched_inputs.clone(),
                        cache_hits: xy.len().saturating_sub(fetched_inputs.len()),
                        cache_misses: fetched_inputs.len(),
                    },
                );
                tiles.extend(fetched_tiles.into_iter().map(WindowTileInput::Encoded));
            }
        }

        (tiles, held_key_guards)
    } else {
        let fetched_inputs = xy.clone();
        let fetched_tiles = fetch_tiles_timed(handle, &xy, perf_stats).await?;
        record_fetch_tiles_call(
            fetch_tiles_debug_log,
            FetchTilesDebugCall {
                uri: uri.to_string(),
                window: debug_window,
                requested_tiles: fetched_inputs.clone(),
                fetched_tiles: fetched_inputs,
                cache_hits: 0,
                cache_misses: xy.len(),
            },
        );
        (
            fetched_tiles
                .into_iter()
                .map(WindowTileInput::Encoded)
                .collect(),
            None,
        )
    };

    let layout = handle.layout;
    let nodata = handle.nodata.unwrap_or(f32::NAN);
    let permit = cpu_sem
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
    let decode_span = tracing::debug_span!("async_tiff.decode_window");
    let decode_started = Instant::now();
    let (out, decoded_tiles) = tokio::task::spawn_blocking(move || {
        let _span = decode_span.enter();
        let mut out = RasterOwned::from_filled_with_layout(
            window.width,
            window.height,
            bands,
            nodata,
            layout,
        );
        let layout_planar = matches!(layout, RasterLayout::Planar);
        let mut cached_tiles = Vec::new();
        let mut encoded_tiles = Vec::new();

        for tile in tiles {
            match tile {
                WindowTileInput::Decoded { tx, ty, arr } => cached_tiles.push((tx, ty, arr)),
                WindowTileInput::Encoded(tile) => encoded_tiles.push(tile),
            }
        }

        let decoded_tiles = if encoded_tiles.len() <= 1 {
            let decoder = DecoderRegistry::default();
            encoded_tiles
                .into_iter()
                .map(|tile| {
                    let tx = tile.x();
                    let ty = tile.y();
                    let _span =
                        tracing::trace_span!("async_tiff.decode_tile", tx = tx, ty = ty).entered();
                    let arr = Arc::new(
                        tile.decode(&decoder)
                            .map_err(|e| GtiError::IndexLoad(e.to_string()))?,
                    );
                    Ok::<_, GtiError>((tx, ty, arr))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            encoded_tiles
                .into_par_iter()
                .map_init(DecoderRegistry::default, |decoder, tile| {
                    let tx = tile.x();
                    let ty = tile.y();
                    let _span =
                        tracing::trace_span!("async_tiff.decode_tile", tx = tx, ty = ty).entered();
                    let arr = Arc::new(
                        tile.decode(decoder)
                            .map_err(|e| GtiError::IndexLoad(e.to_string()))?,
                    );
                    Ok::<_, GtiError>((tx, ty, arr))
                })
                .collect::<Result<Vec<_>>>()?
        };

        for (tx, ty, arr) in cached_tiles.iter().chain(decoded_tiles.iter()) {
            write_tile_into_window(
                arr.as_ref(),
                tx * tile_w,
                ty * tile_h,
                layout_planar,
                window,
                &mut out,
            );
        }

        Ok::<_, GtiError>((out, decoded_tiles))
    })
    .await
    .map_err(|e| GtiError::IndexLoad(e.to_string()))??;
    record_decode_timing(perf_stats, decode_started.elapsed());

    if let Some(cache) = pixel_cache
        && !decoded_tiles.is_empty()
    {
        let mut guard = cache.lock().await;
        for (tx, ty, arr) in &decoded_tiles {
            let key = SourceTileKey::new(uri, *tx, *ty);
            let bytes = decoded_array_byte_len(arr.as_ref());
            guard.put(key, arr.clone(), bytes);
        }
        tracing::debug!(
            target: "mosaic",
            uri = uri,
            src_x = window.x,
            src_y = window.y,
            src_w = window.width,
            src_h = window.height,
            inserted_tiles = decoded_tiles.len(),
            "decoded tile cache insert"
        );
    }

    drop(held_key_guards);
    drop(permit);

    // Build sub-grid aligned to the window.
    let mut affine = handle.src_grid.affine;
    let x = window.x as f64;
    let y = window.y as f64;
    affine.c = affine.a.mul_add(x, affine.b.mul_add(y, affine.c));
    affine.f = affine.d.mul_add(x, affine.e.mul_add(y, affine.f));
    let mut sub_grid = GridSpec::new(window.width, window.height, affine);
    sub_grid.crs = handle.src_grid.crs.clone();

    tracing::info!(
        target: "mosaic",
        uri = %handle.uri,
        src_x = window.x,
        src_y = window.y,
        src_w = window.width,
        src_h = window.height,
        tiles_x = tx1 - tx0 + 1,
        tiles_y = ty1 - ty0 + 1,
        "decode_tile: window read"
    );

    Ok((out, sub_grid))
}

fn write_tile_into_window(
    arr: &async_tiff::Array,
    tile_x: usize,
    tile_y: usize,
    layout_planar: bool,
    window: PixelWindow,
    out: &mut RasterOwned,
) {
    let shape = arr.shape();
    let (tile_h, tile_w, band_count) = if layout_planar {
        (shape[1], shape[2], shape[0])
    } else {
        (shape[0], shape[1], shape[2])
    };

    // Overlap extents in source coords.
    let win_x0 = window.x;
    let win_y0 = window.y;
    let win_x1 = window.x_end();
    let win_y1 = window.y_end();

    let tile_x0 = tile_x;
    let tile_y0 = tile_y;
    let tile_x1 = tile_x + tile_w;
    let tile_y1 = tile_y + tile_h;

    let overlap_x0 = win_x0.max(tile_x0);
    let overlap_y0 = win_y0.max(tile_y0);
    let overlap_x1 = win_x1.min(tile_x1);
    let overlap_y1 = win_y1.min(tile_y1);

    if overlap_x0 >= overlap_x1 || overlap_y0 >= overlap_y1 {
        return;
    }

    for b in 0..band_count {
        for sy in overlap_y0..overlap_y1 {
            let dy_out = sy - win_y0;
            let ty_local = sy - tile_y0;
            for sx in overlap_x0..overlap_x1 {
                let dx_out = sx - win_x0;
                let tx_local = sx - tile_x0;
                let src_idx = if layout_planar {
                    b * (tile_h * tile_w) + ty_local * tile_w + tx_local
                } else {
                    (ty_local * tile_w + tx_local) * band_count + b
                };
                let val = match arr.data() {
                    async_tiff::TypedArray::UInt8(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::UInt16(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::UInt32(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Int8(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Int16(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Int32(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::UInt64(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Int64(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Float32(v) => v[src_idx],
                    async_tiff::TypedArray::Float64(v) => v[src_idx] as f32,
                    async_tiff::TypedArray::Bool(v) => {
                        if v[src_idx] {
                            1.0
                        } else {
                            0.0
                        }
                    }
                };
                let dst_idx = out.index(dx_out, dy_out, b);
                out.data_mut()[dst_idx] = val;
            }
        }
    }
}

/// Cache key for source tile bytes.
#[derive(Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct SourceTileKey {
    uri: String,
    x: u32,
    y: u32,
}

impl SourceTileKey {
    pub fn new(uri: &str, x: usize, y: usize) -> Self {
        Self {
            uri: uri.to_string(),
            x: x as u32,
            y: y as u32,
        }
    }
}

async fn fetch_tiles_timed(
    handle: &TileHandle,
    xy: &[(usize, usize)],
    perf_stats: Option<&PerfStatsSink>,
) -> Result<Vec<Tile>> {
    let started = Instant::now();
    let fetched_tiles = handle
        .ifd
        .fetch_tiles(xy, handle.reader.as_ref())
        .instrument(tracing::debug_span!(
            "async_tiff.fetch_tiles",
            count = xy.len()
        ))
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
    let elapsed = started.elapsed();

    let fetched_bytes = fetched_tiles
        .iter()
        .map(compressed_tile_byte_len)
        .sum::<usize>();
    update_perf_stats(perf_stats, |stats| {
        stats.fetch_tiles_calls = stats.fetch_tiles_calls.saturating_add(1);
        stats.fetch_tiles_tiles = stats.fetch_tiles_tiles.saturating_add(xy.len());
        stats.fetch_tiles_bytes = stats.fetch_tiles_bytes.saturating_add(fetched_bytes);
        stats.fetch_tiles_time += elapsed;
    });

    Ok(fetched_tiles)
}

fn compressed_tile_byte_len(tile: &Tile) -> usize {
    let bytes = match tile.compressed_bytes() {
        CompressedBytes::Chunky(bytes) => bytes.len(),
        CompressedBytes::Planar(band_bytes) => band_bytes.iter().map(|bytes| bytes.len()).sum(),
    };
    bytes.max(1)
}

fn decoded_array_byte_len(arr: &async_tiff::Array) -> usize {
    let bytes = match arr.data() {
        async_tiff::TypedArray::Bool(v) => v.len() * std::mem::size_of::<bool>(),
        async_tiff::TypedArray::UInt8(v) => v.len() * std::mem::size_of::<u8>(),
        async_tiff::TypedArray::UInt16(v) => v.len() * std::mem::size_of::<u16>(),
        async_tiff::TypedArray::UInt32(v) => v.len() * std::mem::size_of::<u32>(),
        async_tiff::TypedArray::UInt64(v) => v.len() * std::mem::size_of::<u64>(),
        async_tiff::TypedArray::Int8(v) => v.len() * std::mem::size_of::<i8>(),
        async_tiff::TypedArray::Int16(v) => v.len() * std::mem::size_of::<i16>(),
        async_tiff::TypedArray::Int32(v) => v.len() * std::mem::size_of::<i32>(),
        async_tiff::TypedArray::Int64(v) => v.len() * std::mem::size_of::<i64>(),
        async_tiff::TypedArray::Float32(v) => v.len() * std::mem::size_of::<f32>(),
        async_tiff::TypedArray::Float64(v) => v.len() * std::mem::size_of::<f64>(),
    };
    bytes.max(1)
}

fn record_decode_timing(perf_stats: Option<&PerfStatsSink>, elapsed: Duration) {
    update_perf_stats(perf_stats, |stats| {
        stats.decode_windows = stats.decode_windows.saturating_add(1);
        stats.decode_time += elapsed;
    });
}

fn update_perf_stats(perf_stats: Option<&PerfStatsSink>, update: impl FnOnce(&mut PerfStats)) {
    let Some(perf_stats) = perf_stats else {
        return;
    };
    let mut guard = match perf_stats.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    update(&mut guard);
}

fn record_fetch_tiles_call(debug_log: Option<&FetchTilesDebugLog>, call: FetchTilesDebugCall) {
    let Some(debug_log) = debug_log else {
        return;
    };
    let mut guard = match debug_log.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.push(call);
}

fn build_affine_and_crs(ifd: &ImageFileDirectory) -> Result<(warp_rs::Affine2D, String)> {
    let scale = ifd
        .model_pixel_scale()
        .ok_or_else(|| GtiError::IndexLoad("missing ModelPixelScaleTag".into()))?;
    let tie = ifd
        .model_tiepoint()
        .ok_or_else(|| GtiError::IndexLoad("missing ModelTiepointTag".into()))?;
    if tie.len() < 6 || scale.len() < 2 {
        return Err(GtiError::IndexLoad(
            "invalid ModelPixelScale/ModelTiepoint".into(),
        ));
    }
    let scale_x = scale[0];
    let scale_y = scale[1];
    let tie_x = tie[3];
    let tie_y = tie[4];
    let tie_col = tie[0];
    let tie_row = tie[1];

    // Standard GeoTIFF transform:
    // Xgeo = tie_x + scale_x * (col - tie_col)
    // Ygeo = tie_y - scale_y * (row - tie_row)
    let a = scale_x;
    let e = -scale_y;
    let c = tie_x - scale_x * tie_col;
    let f = tie_y + scale_y * tie_row;
    let affine = warp_rs::Affine2D::new(a, 0.0, c, 0.0, e, f);

    let crs = ifd
        .geo_key_directory()
        .and_then(|g| g.epsg_code())
        .map(|epsg| format!("EPSG:{epsg}"))
        .ok_or_else(|| GtiError::IndexLoad("missing GeoTIFF CRS".into()))?;

    Ok((affine, crs))
}
