#![allow(dead_code)]

use std::sync::Arc;

use async_tiff::decoder::DecoderRegistry;
use async_tiff::metadata::TiffMetadataReader;
use async_tiff::metadata::cache::ReadaheadMetadataCache;
use async_tiff::reader::{AsyncFileReader, ObjectReader};
use async_tiff::{ImageFileDirectory, TIFF};
use lru::LruCache;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::Mutex;
use url::Url;
use warp_rs::{GridSpec, PixelWindow, RasterLayout, RasterOwned, SourceTileLayout, SourceTiling};

use crate::types::{GtiError, Result, TileMeta};

pub struct TileHandle {
    pub uri: String,
    pub tiff: TIFF,
    pub ifd: ImageFileDirectory,
    pub reader: Arc<dyn AsyncFileReader>,
    pub layout: RasterLayout,
    pub nodata: Option<f32>,
    pub src_grid: GridSpec,
    pub bands: usize,
}

/// Open a tile lazily using a caller-provided `ObjectStore`.
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
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
    let ifds = meta
        .read_all_ifds(&cache)
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
    let tiff = TIFF::new(ifds, meta.endianness());
    let ifd = tiff
        .ifds()
        .first()
        .cloned()
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
        tiff,
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
        width: handle.src_grid.width,
        height: handle.src_grid.height,
        bands: handle.bands,
        src_grid: handle.src_grid.clone(),
        dst_to_src,
        nodata: handle.nodata,
    })
}

/// Read full raster into f32 RasterOwned.
pub async fn read_full_raster_f32(handle: &TileHandle) -> Result<RasterOwned> {
    tracing::info!(
        target: "mosaic",
        uri = %handle.uri,
        width = handle.ifd.image_width(),
        height = handle.ifd.image_height(),
        bands = handle.ifd.samples_per_pixel(),
        "decode_tile: start full read"
    );
    let width = handle.ifd.image_width() as usize;
    let height = handle.ifd.image_height() as usize;
    let bands = handle.ifd.samples_per_pixel() as usize;
    let mut out = RasterOwned::from_filled_with_layout(
        width,
        height,
        bands,
        handle.nodata.unwrap_or(f32::NAN),
        handle.layout,
    );

    let decoder = DecoderRegistry::default();
    let (tiles_x, tiles_y) = handle.ifd.tile_count().unwrap_or((1, 1)); // strips treated as 1xN

    for ty in 0..tiles_y {
        for tx in 0..tiles_x {
            let tile = handle
                .ifd
                .fetch_tile(tx, ty, handle.reader.as_ref())
                .await
                .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
            let arr = tile
                .decode(&decoder)
                .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
            let layout_planar = matches!(handle.layout, RasterLayout::Planar);
            write_tile_into_raster(&arr, tx, ty, layout_planar, &mut out);
        }
    }

    Ok(out)
}

/// Read just the requested pixel window into a RasterOwned.
pub type PixelCache = Arc<Mutex<LruCache<WindowKey, Arc<CachedWindow>>>>;

pub async fn read_window_raster_f32(
    handle: &TileHandle,
    window: PixelWindow,
    uri: &str,
    pixel_cache: Option<&PixelCache>,
) -> Result<(RasterOwned, GridSpec)> {
    // Check cache first.
    if let Some(cache) = pixel_cache {
        let key = WindowKey::new(uri, window);
        if let Some(entry) = cache.lock().await.get(&key) {
            let cached = entry.clone();
            return Ok((cached.raster.as_ref().clone(), cached.grid.clone()));
        }
    }

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

    let mut out = RasterOwned::from_filled_with_layout(
        window.width,
        window.height,
        bands,
        handle.nodata.unwrap_or(f32::NAN),
        handle.layout,
    );

    let decoder = DecoderRegistry::default();
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

    let tiles = handle
        .ifd
        .fetch_tiles(&xy, handle.reader.as_ref())
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))?;

    for tile in tiles {
        let tx = tile.x();
        let ty = tile.y();
        let arr = tile
            .decode(&decoder)
            .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
        let layout_planar = matches!(handle.layout, RasterLayout::Planar);
        write_tile_into_window(
            &arr,
            tx * tile_w,
            ty * tile_h,
            layout_planar,
            window,
            &mut out,
        );
    }

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

    // Populate cache if configured.
    if let Some(cache) = pixel_cache {
        let key = WindowKey::new(uri, window);
        let entry = Arc::new(CachedWindow {
            raster: Arc::new(out.clone()),
            grid: sub_grid.clone(),
        });
        let mut guard = cache.lock().await;
        guard.put(key, entry);
    }

    Ok((out, sub_grid))
}

fn write_tile_into_raster(
    arr: &async_tiff::Array,
    tx: usize,
    ty: usize,
    layout_planar: bool,
    out: &mut RasterOwned,
) {
    let shape = arr.shape();
    // Derive dims based on planar flag
    let (tile_h, tile_w, band_count) = if layout_planar {
        (shape[1], shape[2], shape[0])
    } else {
        (shape[0], shape[1], shape[2])
    };
    let x0 = tx * tile_w;
    let y0 = ty * tile_h;

    for b in 0..band_count {
        for dy in 0..tile_h {
            let sy = dy;
            let dy_out = y0 + dy;
            if dy_out >= out.height() {
                continue;
            }
            for dx in 0..tile_w {
                let sx = dx;
                let dx_out = x0 + dx;
                if dx_out >= out.width() {
                    continue;
                }
                let src_idx = if layout_planar {
                    // planar: [band][y][x]
                    b * (tile_h * tile_w) + sy * tile_w + sx
                } else {
                    // chunky: [y][x][band]
                    (sy * tile_w + sx) * band_count + b
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

/// Cache key for windowed decodes.
#[derive(Hash, PartialEq, Eq, Clone)]
pub struct WindowKey {
    uri: String,
    x: u32,
    y: u32,
    w: u32,
    h: u32,
}

impl WindowKey {
    pub fn new(uri: &str, window: PixelWindow) -> Self {
        Self {
            uri: uri.to_string(),
            x: window.x as u32,
            y: window.y as u32,
            w: window.width as u32,
            h: window.height as u32,
        }
    }
}

/// Cached decode result (window raster + sub-grid).
#[derive(Clone)]
pub struct CachedWindow {
    pub raster: Arc<RasterOwned>,
    pub grid: GridSpec,
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
