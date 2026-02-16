# mosaic-index plan

## Goal
- Provide a practical Rust crate for building mosaics from an indexed set of GeoTIFF/COG tiles.
- Keep API centered on a single call: `build_mosaic`.
- Focus optimization on remote I/O, decode reuse, and predictable concurrency.

## Current pipeline
1. Validate `MosaicSpec` and build destination grid/window.
2. Filter and sort `TileRecord` candidates.
3. Process destination blocks in parallel (`max_tile_concurrency`).
4. For each overlapping source tile/window:
- Open tile handle lazily (metadata cache).
- Plan source window (`warp_rs::plan_reproject_work`).
- Read source tiles via `async_tiff::fetch_tiles`.
- Decode source tiles into raster window.
- Reproject into destination block (`warp_rs::reproject`).
5. Composite block outputs and stop early per block when fully covered.

## Key learnings from this session
- Tile dedupe is working when `fetched_total == unique_fetched`.
- With cache effectively off (`meta_max_bytes=0`, `pixel_max_bytes=0`), we saw repeated fetches:
- `calls=31, requested_total=91, fetched_total=91, unique_fetched=36`, wall `~58s`.
- This proved many logical tile requests map to a much smaller unique tile set.
- Perf counters showed decode was a major bottleneck in addition to fetch:
- `fetch_time_ms~129k`, `decode_time_ms~133k`, `reproject_time_ms~3.5k` (aggregated across concurrency).
- Caching compressed tiles reduced duplicate network fetches but still repeated decode work.
- Switching pixel cache to decoded tile arrays improved wall time materially:
- Example run: `calls=16, requested_total=49, fetched_total=36, unique_fetched=36`, wall `~35.5s`.
- Concurrency matters even with good cache behavior:
- `max_tile_concurrency=1` serialized pipeline and produced very slow wall time (`~157.8s`).
- Higher settings (`max_tile_concurrency=32`, `max_work_concurrency=16`) improved best observed wall time to `~33.4s`.

## Current best-known direction
- Keep decoded tile cache enabled (byte-bounded LRU).
- Keep per-tile singleflight guards to avoid duplicate fetch/decode under contention.
- Tune concurrency per environment; `32/16` is the best observed setting in this session.

## Near-term follow-ups
- Add explicit decoded cache hit/miss counters to perf stats.
- Keep comparing wall time across a small concurrency sweep on target hardware.
- Consider documenting recommended cache sizing based on expected tile dimensions and band count.
