mod cache;
mod compose;
pub mod index;
mod io;
pub mod planner;
mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, collections::hash_map::Entry};

use futures::{StreamExt, TryStreamExt, stream};
use object_store::ObjectStore;
use tokio::sync::{Mutex, Semaphore};
use warp_rs::{CoordinateTransform, GridSpec, PixelWindow, WarpWorkTile};

pub use types::{
    BBox, BuildOptions, CacheConfig, DataType, GtiError, MosaicSpec, OutputWindow, Result,
    SortValue, TileRecord,
};

#[cfg(feature = "geoparquet")]
pub use index::load_tiles_from_geoparquet;
pub use warp_rs::{IdentityTransform, RasterOwned, Resample};

type MetaCache = Arc<Mutex<cache::ByteLruCache<String, Arc<io::TileHandle>>>>;
type MetaOpenGuards = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

#[derive(Clone, Copy, Debug)]
struct BlockWork {
    x: usize,
    y: usize,
    width: usize,
    height: usize,
}

#[derive(Clone)]
struct BlockExecCtx {
    tiles: Arc<Vec<TileRecord>>,
    object_store: Arc<dyn ObjectStore>,
    meta_cache: Option<MetaCache>,
    meta_open_guards: Option<MetaOpenGuards>,
    pixel_cache: Option<io::PixelCache>,
    dst_grid: Arc<GridSpec>,
    dst_crs: String,
    expected_bands: usize,
    output_nodata: f32,
    resample: Resample,
    z_limit: Option<usize>,
    cpu_sem: Arc<Semaphore>,
    reads_in_flight: Arc<AtomicUsize>,
    reprojects_in_flight: Arc<AtomicUsize>,
}

/// Synchronous wrapper around [`build_mosaic_async`].
pub fn build_mosaic(
    spec: &MosaicSpec,
    tiles: impl IntoIterator<Item = TileRecord>,
    opts: BuildOptions,
) -> Result<RasterOwned> {
    match opts.tokio_handle.clone() {
        Some(handle) => handle.block_on(build_mosaic_async(spec, tiles, opts)),
        None => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| GtiError::IndexLoad(e.to_string()))?
            .block_on(build_mosaic_async(spec, tiles, opts)),
    }
}

/// Async mosaic build entry point.
///
/// Execution model:
/// - filter/sort index rows once;
/// - partition destination into output blocks;
/// - process blocks in parallel;
/// - inside each block, walk source tiles in z-order and stop early once block has no nodata.
pub async fn build_mosaic_async(
    spec: &MosaicSpec,
    tiles: impl IntoIterator<Item = TileRecord>,
    opts: BuildOptions,
) -> Result<RasterOwned> {
    spec.validate()?;
    tracing::info!(target: "mosaic", "build_mosaic: start");

    let dst = planner::build_destination(spec)?;
    tracing::info!(
        target: "mosaic",
        width = dst.grid.width,
        height = dst.grid.height,
        window_w = dst.window.width,
        window_h = dst.window.height,
        "destination built"
    );

    let tiles = index::filter_and_sort_tiles(tiles, spec, &dst);
    tracing::info!(target: "mosaic", tile_count = tiles.len(), "tiles filtered/sorted");

    let mut raster = compose::allocate_output(&dst.window, spec.band_count, spec.output_nodata)?;

    let (meta_cache, meta_open_guards, pixel_cache) = build_caches(opts.cache);
    let block_concurrency = opts.max_tile_concurrency.max(1);
    let cpu_concurrency = opts.max_work_concurrency.max(1);
    let cpu_sem = Arc::new(Semaphore::new(cpu_concurrency));
    let blocks = plan_blocks(
        dst.grid.width,
        dst.grid.height,
        spec.blockxsize,
        spec.blockysize,
    );
    let total_blocks = blocks.len();

    let exec_ctx = BlockExecCtx {
        tiles: Arc::new(tiles),
        object_store: opts.object_store.clone(),
        meta_cache,
        meta_open_guards,
        pixel_cache,
        dst_grid: Arc::new(dst.grid.clone()),
        dst_crs: dst.grid.crs.clone().unwrap_or_default(),
        expected_bands: spec.band_count as usize,
        output_nodata: spec.output_nodata,
        resample: spec.resampling,
        z_limit: opts.z_limit,
        cpu_sem,
        reads_in_flight: Arc::new(AtomicUsize::new(0)),
        reprojects_in_flight: Arc::new(AtomicUsize::new(0)),
    };

    let mut blocks_filled = 0usize;
    let mut block_stream = stream::iter(blocks.into_iter())
        .map(|block| {
            let exec_ctx = exec_ctx.clone();
            async move { process_block(exec_ctx, block).await }
        })
        .buffer_unordered(block_concurrency);

    while let Some(block_result) = block_stream.try_next().await? {
        let (block, block_raster, is_filled) = block_result;
        {
            let _span =
                tracing::trace_span!("compose.blit_block", block_x = block.x, block_y = block.y)
                    .entered();
            compose::blit_block(
                &block_raster,
                &mut raster,
                block.x,
                block.y,
                spec.output_nodata,
            );
        }
        if is_filled {
            blocks_filled += 1;
        }
    }

    tracing::info!(
        target: "mosaic",
        blocks_filled,
        total_blocks,
        "build_mosaic: complete"
    );

    Ok(raster)
}

fn build_caches(
    cfg: Option<CacheConfig>,
) -> (
    Option<MetaCache>,
    Option<MetaOpenGuards>,
    Option<io::PixelCache>,
) {
    let Some(cfg) = cfg else {
        return (None, None, None);
    };
    let meta_cap = cfg.meta_max_bytes.max(1);
    let pixel_cap = cfg.pixel_max_bytes.max(1);

    (
        Some(Arc::new(Mutex::new(cache::ByteLruCache::<
            String,
            Arc<io::TileHandle>,
        >::new(meta_cap)))),
        Some(Arc::new(Mutex::new(HashMap::new()))),
        Some(Arc::new(Mutex::new(cache::ByteLruCache::<
            io::WindowKey,
            Arc<io::CachedWindow>,
        >::new(pixel_cap)))),
    )
}

fn plan_blocks(grid_width: usize, grid_height: usize, blockx: u32, blocky: u32) -> Vec<BlockWork> {
    let bx = blockx.max(1) as usize;
    let by = blocky.max(1) as usize;
    let mut blocks = Vec::new();

    let mut y = 0usize;
    while y < grid_height {
        let height = by.min(grid_height - y);
        let mut x = 0usize;
        while x < grid_width {
            let width = bx.min(grid_width - x);
            blocks.push(BlockWork {
                x,
                y,
                width,
                height,
            });
            x += bx;
        }
        y += by;
    }

    blocks
}

#[tracing::instrument(
    name = "mosaic.block",
    skip(exec_ctx),
    fields(block_x = block.x, block_y = block.y)
)]
async fn process_block(
    exec_ctx: BlockExecCtx,
    block: BlockWork,
) -> Result<(BlockWork, RasterOwned, bool)> {
    tracing::info!(
        target: "mosaic",
        block_x = block.x,
        block_y = block.y,
        block_w = block.width,
        block_h = block.height,
        "block: start"
    );
    let mut block_raster = RasterOwned::from_filled(
        block.width,
        block.height,
        exec_ctx.expected_bands,
        exec_ctx.output_nodata,
    );

    let block_work = WarpWorkTile {
        dst_x: block.x,
        dst_y: block.y,
        dst_width: block.width,
        dst_height: block.height,
        src_window: None,
    };
    let dst_block_grid = planner::block_subgrid(exec_ctx.dst_grid.as_ref(), &block_work);
    let mut overlaps_considered = 0usize;

    for tile in exec_ctx.tiles.iter() {
        let handle = get_or_open_handle(
            &tile.location,
            exec_ctx.object_store.clone(),
            exec_ctx.meta_cache.as_ref(),
            exec_ctx.meta_open_guards.as_ref(),
        )
        .await?;
        let meta = io::tile_meta_from_handle(&handle, &exec_ctx.dst_crs)?;
        if meta.bands != exec_ctx.expected_bands {
            return Err(GtiError::InvalidSpec(format!(
                "tile band count mismatch for {}: expected {}, got {}",
                tile.location, exec_ctx.expected_bands, meta.bands
            )));
        }

        let Some(src_window) = ({
            let _span = tracing::debug_span!("planner.src_window").entered();
            plan_src_window_for_block(
                &meta.src_grid,
                &dst_block_grid,
                exec_ctx.resample,
                meta.dst_to_src.as_ref(),
            )?
        }) else {
            continue;
        };

        if let Some(limit) = exec_ctx.z_limit
            && overlaps_considered >= limit
        {
            break;
        }
        overlaps_considered += 1;

        let reads_now = exec_ctx.reads_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::debug!(
            target: "mosaic",
            block_x = block.x,
            block_y = block.y,
            uri = %tile.location,
            reads_in_flight = reads_now,
            src_x = src_window.x,
            src_y = src_window.y,
            src_w = src_window.width,
            src_h = src_window.height,
            "block: read start"
        );
        let (raster_src, src_grid) = io::read_window_raster_f32(
            &handle,
            src_window,
            &tile.location,
            exec_ctx.pixel_cache.as_ref(),
            &exec_ctx.cpu_sem,
        )
        .await?;
        let reads_now = exec_ctx.reads_in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
        tracing::debug!(
            target: "mosaic",
            block_x = block.x,
            block_y = block.y,
            uri = %tile.location,
            reads_in_flight = reads_now,
            "block: read done"
        );

        let permit = exec_ctx
            .cpu_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
        let dst_block_grid_for_reproject = dst_block_grid.clone();
        let resample = exec_ctx.resample;
        let reproject_span = tracing::debug_span!(
            "warp.reproject_block",
            block_x = block.x,
            block_y = block.y,
            uri = %tile.location
        );
        let reprojects_now = exec_ctx.reprojects_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::debug!(
            target: "mosaic",
            block_x = block.x,
            block_y = block.y,
            uri = %tile.location,
            reprojects_in_flight = reprojects_now,
            "block: reproject start"
        );
        let reprojected = tokio::task::spawn_blocking(move || {
            let _span = reproject_span.enter();
            let src_view = warp_rs::RasterView::try_new_with_layout(
                raster_src.data(),
                src_grid.width,
                src_grid.height,
                meta.bands,
                raster_src.layout(),
            )?;
            warp_rs::reproject(
                src_view,
                &src_grid,
                &dst_block_grid_for_reproject,
                resample,
                meta.dst_to_src.as_ref(),
                meta.nodata,
                warp_rs::NodataPolicy::PixelStrict,
            )
        })
        .await
        .map_err(|e| GtiError::IndexLoad(e.to_string()))??;
        let reprojects_now = exec_ctx.reprojects_in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
        tracing::debug!(
            target: "mosaic",
            block_x = block.x,
            block_y = block.y,
            uri = %tile.location,
            reprojects_in_flight = reprojects_now,
            "block: reproject done"
        );
        drop(permit);

        if compose::blit_block(
            &reprojected,
            &mut block_raster,
            0,
            0,
            exec_ctx.output_nodata,
        ) {
            tracing::debug!(
                target: "mosaic",
                dst_x = block.x,
                dst_y = block.y,
                uri = %tile.location,
                "block filled; stop evaluating lower-priority tiles"
            );
            tracing::info!(
                target: "mosaic",
                block_x = block.x,
                block_y = block.y,
                overlaps_considered,
                "block: complete (filled)"
            );
            return Ok((block, block_raster, true));
        }
    }

    tracing::info!(
        target: "mosaic",
        block_x = block.x,
        block_y = block.y,
        overlaps_considered,
        "block: complete (partial)"
    );
    Ok((block, block_raster, false))
}

async fn get_or_open_handle(
    location: &str,
    object_store: Arc<dyn ObjectStore>,
    cache: Option<&MetaCache>,
    open_guards: Option<&MetaOpenGuards>,
) -> Result<Arc<io::TileHandle>> {
    let Some(cache) = cache else {
        tracing::debug!(
            target: "mosaic",
            uri = location,
            "meta cache disabled; opening tile"
        );
        return Ok(Arc::new(io::open_tile(location, object_store).await?));
    };

    if let Some(existing) = cache.lock().await.get_cloned(location) {
        tracing::debug!(target: "mosaic", uri = location, "meta cache hit");
        return Ok(existing);
    }
    tracing::debug!(target: "mosaic", uri = location, "meta cache miss");

    let Some(open_guards) = open_guards else {
        let opened = Arc::new(io::open_tile(location, object_store).await?);
        let mut guard = cache.lock().await;
        if let Some(existing) = guard.get_cloned(location) {
            tracing::debug!(
                target: "mosaic",
                uri = location,
                "meta cache hit after open (racing miss)"
            );
            return Ok(existing);
        }
        guard.put(
            location.to_string(),
            opened.clone(),
            estimate_meta_entry_bytes(&opened),
        );
        tracing::debug!(target: "mosaic", uri = location, "meta cache insert");
        return Ok(opened);
    };

    let key_lock = {
        let mut guard = open_guards.lock().await;
        match guard.entry(location.to_string()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Arc::new(Mutex::new(()))).clone(),
        }
    };

    let _open_guard = key_lock.lock().await;
    if let Some(existing) = cache.lock().await.get_cloned(location) {
        tracing::debug!(
            target: "mosaic",
            uri = location,
            "meta cache hit after waiting on singleflight"
        );
        return Ok(existing);
    }

    let opened = Arc::new(io::open_tile(location, object_store).await?);
    cache.lock().await.put(
        location.to_string(),
        opened.clone(),
        estimate_meta_entry_bytes(&opened),
    );
    tracing::debug!(target: "mosaic", uri = location, "meta cache insert");
    Ok(opened)
}

fn estimate_meta_entry_bytes(handle: &io::TileHandle) -> usize {
    // Rough bound for TIFF metadata + handle state; keeps cache budgeting conservative.
    std::mem::size_of::<io::TileHandle>() + handle.uri.len() + 64 * 1024
}

fn plan_src_window_for_block(
    src_grid: &GridSpec,
    dst_block_grid: &GridSpec,
    resample: Resample,
    dst_to_src: &dyn CoordinateTransform,
) -> Result<Option<PixelWindow>> {
    let mut work = warp_rs::plan_reproject_work(
        src_grid,
        dst_block_grid,
        dst_block_grid.width,
        dst_block_grid.height,
        resample,
        dst_to_src,
    )?;

    if let Some(entry) = work.pop() {
        Ok(entry.src_window)
    } else {
        Ok(None)
    }
}
