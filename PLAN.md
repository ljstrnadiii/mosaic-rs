# Minimal GTI-style Mosaic Builder Crate

## Summary
- Build a small `gti` crate (in this repo for now) that consumes a prepared tile index plus explicit mosaic parameters and produces an in-memory f32 mosaic (or a requested output window).
- Delegate resampling to `warp-rs`; use `async-tiff` only when reading needed pixel windows; no pre-scan of tiles.
- Keep planning logic in `gti`, leaving `warp-rs` focused on resampling; expose a single call suited for Python bindings to materialize a specified x/y output window (default full extent).

## Public API
- `struct OutputWindow { x_off: u32, y_off: u32, width: u32, height: u32 }` (offset/size in destination *pixel* coordinates; optional; defaults to full mosaic; e.g., `0,0,1024,1024` means the top-left 1024×1024 pixels, not world units).
- `struct MosaicSpec { resx, resy, bbox: BBox, dst_crs: String, band_count: u16, data_type: DataType, blockxsize: u32, blockysize: u32, resampling: Resample, sort_ascending: bool, output_nodata: f32, window: Option<OutputWindow> }`
- `struct TileRecord { location: String, footprint_4326: geo::MultiPolygon<f64>, sort_key: Option<SortValue> }` where `SortValue` covers string/number/datetime.
- `struct BuildOptions { tokio_handle: Option<tokio::runtime::Handle>, object_store: Arc<dyn object_store::ObjectStore>, max_tile_concurrency: usize, max_work_concurrency: usize, cache: Option<CacheConfig> }`
- `fn build_mosaic(spec: &MosaicSpec, tiles: impl IntoIterator<Item = TileRecord>, opts: BuildOptions) -> Result<warp_rs::RasterOwned, GtiError>` (respects `spec.window` when provided).
- `Resample` reuses `warp_rs::Resample` (Nearest | Bilinear); error on unsupported inputs.
- Output is f32 `RasterOwned` with layout matching `block*` tiling (full array contiguous) sized to the requested window.

## Crate Layout
- `types.rs`: BBox, MosaicSpec, TileRecord, SortValue, errors.
- `index.rs`: tile filtering & sorting, polygon bbox cache.
- `planner.rs`: destination grid build, work block generation, tile→block overlap detection.
- `io.rs`: lazy tile opener via `async-tiff` (object_store feature gated), GeoTIFF metadata extraction (CRS, geotransform, size).
- `compose.rs`: per-block execution, masking/nodata, z-order compositing.
- `lib.rs`: re-exports and `build_mosaic`.

## Data Flow / Algorithm
1. Build destination grid from `bbox` and `resx/resy`; width/height = ceil((maxx−minx)/resx) etc; affine origin at `bbox.minx/miny`, positive resx, negative resy.
2. Transform destination AOI bbox to EPSG:4326 (proj) to cull tiles quickly when possible (still used if only bboxes are available).
3. Ingest GeoParquet tile index (geometry column) into memory; transform destination AOI to the same CRS (assumed EPSG:4326) and perform polygon `Intersects` (skip bbox-only shortcut when polygon exists). For now we assume the Arrow table fits in memory (≈100k tiles is fine); later we can push down queries via Polars/DuckDB/SedonaDB if needed.
4. Sort remaining tiles by `sort_key` and `sort_ascending`; tie-break by input order.
5. Partition destination (or requested output window) into work blocks of `blockxsize/blockysize` using row-major ordering.
6. For each block, find intersecting tiles via polygon `Intersects` from the in-memory index geometries (no covering pass, no bbox shortcut). Do **not** fetch tile content/metadata yet—use only the loaded polygons to avoid 100k remote requests.
7. For each tile/block (inside the work unit only):
   - Lazily open tile with `async-tiff` on first use; read GeoTIFF transform/size and CRS; cache (one request per touched tile).
   - If tile footprint is available, refine intersects using the already-fetched geometry before reading pixels to guard against false positives.
   - Build dst→tile CRS transform (proj).
   - Use `warp_rs::plan_reproject_work` to get source window (expanded for bilinear support).
   - Read that window via `async-tiff` APIs (object_store-backed) to f32 + optional mask/alpha; I/O concurrency bounded by a semaphore.
   - In the same Rayon pool worker, immediately call `warp_rs::reproject` with dst→tile transform and write into the block buffer to avoid extra hops between pools/threads.
8. Composite tiles in z-order: earlier render first, later overwrite where mask is opaque; initialize blocks with `output_nodata`.
9. Per-block early stop: once every pixel in a block has been written with valid data, skip remaining tiles for that block. Stitch blocks into final `RasterOwned` and return.

## Observability / Logging
- Emit structured logs for each pipeline stage: index load (row counts), tile open (URI, CRS, size), work planning (work tile counts), block scheduling, tile decode/reproject, and per-block early-stop triggers.
- Log concurrency: in-flight I/O permits, decode/reproject tasks, and open-handle LRU hits/misses to show parallelism over units of work.
- For tests/examples, surface a concise summary: tiles loaded, blocks planned, tiles actually touched, blocks early-stopped, and total wall time.

## Index Backend Options
- Default: ingest tile records (location + footprint) from caller into an in-memory R-tree (`rstar`) built on bbox; this easily handles ~100k tiles without heavy deps.
- GeoParquet ingest (preferred): read the geometry column into memory (assume EPSG:4326), build polygon objects, and intersect directly (skipping bbox shortcut when polygons are available). We can use Polars/Arrow to load; keep batch size reasonable but allow full in-memory load for typical sizes.
- Backends stay pluggable via a `TileIndexSource` trait (in-memory vectors, Parquet/Arrow scan via Polars/DuckDB, or user-supplied iterator); polygon intersects executed in Rust (`geo::Intersects`) without needing a covering/quadbin pass.
- Covering-based prefilter is optional and currently skipped.

## Caching Strategy
- Layered byte-range cache for tile fetches inspired by async-geotiff Obspec middleware: wrap the object_store client in an LRU range cache (key: path,start,len). Expose hit/miss metrics and configurable max bytes.
- Keep async-tiff read caching minimal in core; let callers inject their own cache wrapper to align with python async-geotiff’s approach.
- Consider cooperative prefetch: when plan requests a window, expand to block-aligned ranges to improve cache locality for neighboring reads.

## Work Scheduling for Cache Locality
- Default block-major row/col traversal (dense tiles expected).
- Maintain a small LRU of open tile handles/windows; prefer scheduling blocks that touch tiles already in the LRU to increase cache hits (configurable window size).
- Make scheduling pluggable: default heuristic as above; allow caller to opt into “tile-major” traversal (iterate tiles, then their overlapping blocks) when tile reads dominate cost.

## Python / async-geotiff Alignment
- Keep cache and store layers injectable so Python wrappers (similar to async-geotiff + Obspec) can pass a cached object_store client and observe stats.
- Avoid GDAL dependencies; rely on `proj` and `geo` only. Expose a thin FFI surface later (pyo3 or napi) that mirrors `build_mosaic` inputs.
- Match async-geotiff’s window + mask semantics: return f32 array plus optional mask per block; Python wrapper can expose NumPy masked arrays.

## Naming / Packaging
- Working name: `mosaic-index` (clear, non-GDAL). If crates.io name conflict arises, fall back to `mosaic-core` or `mosaic-rs`.
- Plan assumes a standalone crate (e.g., sibling repo/dir) decoupled from `warp-rs`; wiring to `warp-rs` via dependency.

## Dependencies
- `warp-rs` (resample/planner)
- `async-tiff` (object_store optional)
- `proj` (required)
- `geo` (polygons/bboxes)
- `rayon` for CPU; `tokio` for async bridge inside sync API.

## Error Handling / Edge Cases
- Reject `resx/resy <= 0`, empty bbox, or width/height over a safety cap.
- Error if tile CRS missing in TIFF tags; configurable skip later.
- Unsupported data types -> error; everything converted to f32.
- If proj transform fails for a tile, skip with warning or bubble error (config flag).
- Masking: prefer alpha band; else all pixels valid; fill `output_nodata` where uncovered.

## Testing
- Unit: bbox→grid math; tile sorting; tile/block intersection with polygons; dst→4326 filtering; window clipping math (x/y offsets + width/height).
- Integration: small fixtures (2 tiles, differing CRS) verifying extent, nodata fill, z-order overlap; bilinear vs nearest sample correctness; partial window materialization returns expected shape/data.
- Async: mock object_store or local file to ensure lazy open; ensure filtered-out tiles aren’t opened.

## Assumptions / Defaults
- Tile index geometries are in EPSG:4326; destination CRS provided by caller.
- Tile CRS read from GeoTIFF metadata at read time; none in index.
- Caller supplies single output nodata value; per-tile nodata not propagated.
- Resampling limited to Nearest/Bilinear.
- API is synchronous but requires a tokio runtime (or creates one internally); remote storage hits only on needed tiles.
- Tiles are typically dense over the AOI; row-major block traversal is sufficient (no Hilbert ordering assumed).
- Output can be clipped to a requested destination window (x/y offset + width/height in pixel space); default is full mosaic.

## Current Design Choices (kept in sync with code)
- Caller must provide an `object_store` in `BuildOptions`; we no longer construct stores per-URI (`parse_url` removed) to reuse a single client/bucket across threads.
- Band-count contract is strict: we fail fast if any tile’s `SamplesPerPixel` differs from `spec.band_count`.
- Windowed reads only: for each planned work tile we fetch/decode just the needed TIFF tiles via `async-tiff::fetch_tiles`, avoiding full-image downloads.
- Bounded async pipeline: tiles run with a concurrency cap (`max_tile_concurrency`) when their AOI bboxes don’t overlap; each tile’s work items also bounded (`max_work_concurrency`) via `buffer_unordered`; each work item fetches → decodes → reprojects, then the main thread composites results. This overlaps I/O and CPU while limiting memory while preserving z-order.
- Logging includes per-window source/destination extents so users can confirm that real data is fetched/decoded/reprojected (helps debug e2e tests).
