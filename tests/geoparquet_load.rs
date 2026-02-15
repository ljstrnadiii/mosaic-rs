#![cfg(feature = "geoparquet")]

use mosaic_index::load_tiles_from_geoparquet;

#[test]
fn load_small_subset_from_index_parquet() {
    // Uses the repo-provided index.parquet; only reads a small subset to keep test light.
    let tiles = load_tiles_from_geoparquet("index.parquet", "geometry", "url", None, Some(10))
        .expect("load tiles");

    assert!(!tiles.is_empty(), "expected some tiles");
    assert!(tiles.len() <= 10);
    for t in &tiles {
        assert!(!t.location.is_empty());
        assert!(!t.footprint_4326.0.is_empty());
    }
}
