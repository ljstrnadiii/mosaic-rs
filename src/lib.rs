mod compose;
pub mod index;
mod io;
pub mod planner;
mod types;

use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use lru::LruCache;
use tokio::sync::Mutex;

pub use types::{
    BBox, BuildOptions, CacheConfig, DataType, GtiError, MosaicSpec, OutputWindow, Result,
    SortValue, TileRecord,
};

#[cfg(feature = "geoparquet")]
pub use index::load_tiles_from_geoparquet;
pub use warp_rs::{IdentityTransform, RasterOwned, Resample};

/// Build a mosaic (or requested output window) from the provided tile index and mosaic spec.
///
/// This is an initial synchronous implementation that lays out the destination grid,
/// validates inputs, and returns a nodata-filled raster sized to the requested window.
/// Rendering, I/O, and reprojection will be layered in subsequent iterations.
pub fn build_mosaic(
    spec: &MosaicSpec,
    tiles: impl IntoIterator<Item = TileRecord>,
    _opts: BuildOptions,
) -> Result<RasterOwned> {
    spec.validate()?;
    tracing::info!(target: "mosaic", "build_mosaic: start");

    // Prepare destination grid + requested window.
    let dst = planner::build_destination(spec)?;
    tracing::info!(target: "mosaic", width = dst.grid.width, height = dst.grid.height, window_w = dst.window.width, window_h = dst.window.height, "destination built");

    // Filter/sort tiles for the AOI.
    let mut tiles: Vec<TileRecord> = index::filter_and_sort_tiles(tiles, spec, &dst);
    if let Some(limit) = _opts.z_limit
        && tiles.len() > limit
    {
        tiles.truncate(limit);
    }
    tracing::info!(target: "mosaic", tile_count = tiles.len(), "tiles filtered/sorted");

    // Allocate output raster filled with nodata.
    let mut raster = compose::allocate_output(&dst.window, spec.band_count, spec.output_nodata)?;

    // Build or reuse a tokio runtime.
    let runtime = match _opts.tokio_handle {
        Some(h) => RuntimeOrHandle::Handle(h),
        None => RuntimeOrHandle::Owned(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| GtiError::IndexLoad(e.to_string()))?,
        ),
    };

    // Per-block early stop tracking.
    let mut block_filled = std::collections::HashSet::new();
    let total_blocks = {
        let bx = spec.blockxsize.max(1) as usize;
        let by = spec.blockysize.max(1) as usize;
        let nx = (dst.window.width as usize).div_ceil(bx);
        let ny = (dst.window.height as usize).div_ceil(by);
        nx * ny
    };

    // Process tiles in z-order, but execute per-tile work asynchronously with bounded concurrency.
    let resample = spec.resampling;
    let max_tile_conc = _opts.max_tile_concurrency.max(1);
    let max_work_conc = _opts.max_work_concurrency.max(1);
    let tiles: Vec<TileRecord> = tiles.into_iter().collect();

    // Build caches from config.
    let (meta_cache, pixel_cache) = if let Some(cfg) = _opts.cache {
        let meta_cap = NonZeroUsize::new(cfg.meta_capacity.max(1)).unwrap();
        let pix_cap = NonZeroUsize::new(cfg.pixel_capacity.max(1)).unwrap();
        (
            Some(Arc::new(Mutex::new(
                LruCache::<String, Arc<io::TileHandle>>::new(meta_cap),
            ))),
            Some(Arc::new(Mutex::new(LruCache::<
                io::WindowKey,
                Arc<io::CachedWindow>,
            >::new(pix_cap)))),
        )
    } else {
        (None, None)
    };

    // Run the pipeline inside one async block; process tiles in order, but each tile's work runs with bounded concurrency.
    runtime.block_on(async {
        let tile_results: Vec<(usize, Vec<(planner::PlannedTileWork, RasterOwned)>)> =
            stream::iter(tiles.into_iter().enumerate())
                .map(|(tile_idx, tile)| {
                    let object_store = _opts.object_store.clone();
                    let dst = dst.clone();
                    let meta_cache = meta_cache.clone();
                    let pixel_cache = pixel_cache.clone();
                    async move {
                        // Meta cache lookup or open.
                        let handle = if let Some(cache) = &meta_cache {
                            let mut guard = cache.lock().await;
                            if let Some(h) = guard.get(&tile.location) {
                                h.clone()
                            } else {
                                let h = Arc::new(
                                    crate::io::open_tile(&tile.location, object_store.clone())
                                        .await?,
                                );
                                guard.put(tile.location.clone(), h.clone());
                                h
                            }
                        } else {
                            Arc::new(
                                crate::io::open_tile(&tile.location, object_store.clone()).await?,
                            )
                        };

                        let meta = crate::io::tile_meta_from_handle(
                            &handle,
                            &dst.grid.crs.clone().unwrap_or_default(),
                        )?;
                        if meta.bands != spec.band_count as usize {
                            return Err(GtiError::InvalidSpec(format!(
                                "tile band count mismatch for {}: expected {}, got {}",
                                tile.location, spec.band_count, meta.bands
                            )));
                        }

                        // Plan work for this tile only.
                        let tile_geom = planner::TileGeometry {
                            src_grid: meta.src_grid.clone(),
                            dst_to_src: meta.dst_to_src.clone(),
                        };
                        let work_items = planner::plan_tile_reprojects(
                            &dst,
                            &[tile_geom],
                            spec.blockxsize,
                            spec.blockysize,
                            spec.resampling,
                        )?;

                        let work_items: Vec<_> = work_items
                            .into_iter()
                            .filter(|w| w.work.src_window.is_some())
                            .collect();

                        if work_items.is_empty() {
                            return Ok((tile_idx, Vec::new()));
                        }

                        tracing::info!(
                            target: "mosaic",
                            tile_idx,
                            work_items = work_items.len(),
                            uri = %tile.location,
                            "tile planned"
                        );

                        let dst_grid = dst.grid.clone();
                        let pixel_cache = pixel_cache.clone();
                        let uri = tile.location.clone();

                        let work_results: Vec<(planner::PlannedTileWork, RasterOwned)> =
                            stream::iter(work_items.into_iter())
                                .map(|work| {
                                    let handle = handle.clone();
                                    let meta = meta.clone();
                                    let dst_grid = dst_grid.clone();
                                    let pixel_cache = pixel_cache.clone();
                                    let uri = uri.clone();
                                    async move {
                                        let src_window = work.work.src_window.unwrap();
                                        let (raster_src, src_grid) =
                                            crate::io::read_window_raster_f32(
                                                &handle,
                                                src_window,
                                                &uri,
                                                pixel_cache.as_ref(),
                                            )
                                            .await?;

                                        let dst_block_grid =
                                            planner::block_subgrid(&dst_grid, &work.work);

                                        let block_raster = tokio::task::spawn_blocking(move || {
                                            let src_view =
                                                warp_rs::RasterView::try_new_with_layout(
                                                    raster_src.data(),
                                                    src_grid.width,
                                                    src_grid.height,
                                                    meta.bands,
                                                    raster_src.layout(),
                                                )?;
                                            warp_rs::reproject(
                                                src_view,
                                                &src_grid,
                                                &dst_block_grid,
                                                resample,
                                                meta.dst_to_src.as_ref(),
                                                meta.nodata,
                                                warp_rs::NodataPolicy::PixelStrict,
                                            )
                                        })
                                        .await
                                        .map_err(|e| GtiError::IndexLoad(e.to_string()))??;

                                        Ok::<_, GtiError>((work, block_raster))
                                    }
                                })
                                .buffer_unordered(max_work_conc)
                                .try_collect()
                                .await?;

                        Ok::<_, GtiError>((tile_idx, work_results))
                    }
                })
                .buffer_unordered(max_tile_conc)
                .try_collect()
                .await?;

        // Apply results in tile order to preserve z-order; skip blocks already filled.
        let mut tile_results = tile_results;
        tile_results.sort_by_key(|(i, _)| *i);
        for (_idx, work_results) in tile_results.into_iter() {
            for (work, block_raster) in work_results {
                let block_id = (work.work.dst_x, work.work.dst_y);
                if block_filled.contains(&block_id) {
                    continue;
                }
                let block_complete = compose::blit_block(
                    &block_raster,
                    &mut raster,
                    work.work.dst_x,
                    work.work.dst_y,
                    spec.output_nodata,
                );
                if block_complete {
                    block_filled.insert(block_id);
                }
                if block_filled.len() == total_blocks {
                    break;
                }
            }
            if block_filled.len() == total_blocks {
                break;
            }
        }
        Ok::<_, GtiError>(())
    })?;
    tracing::info!(target: "mosaic", blocks_filled = block_filled.len(), "build_mosaic: complete");

    Ok(raster)
}

enum RuntimeOrHandle {
    Owned(tokio::runtime::Runtime),
    Handle(tokio::runtime::Handle),
}

impl RuntimeOrHandle {
    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        match self {
            RuntimeOrHandle::Owned(rt) => rt.block_on(fut),
            RuntimeOrHandle::Handle(h) => h.block_on(fut),
        }
    }
}
