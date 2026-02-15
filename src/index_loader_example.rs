//! Example helper for loading a GeoParquet tile index and calling `build_mosaic`.
//!
//! This stays out of the public API; users can copy/adapt or enable the `geoparquet` feature.
//!
//! Not compiled by default; kept as inline documentation/example.
#![allow(dead_code)]

#[cfg(feature = "geoparquet")]
pub async fn example_usage() -> mosaic_index::Result<()> {
    use mosaic_index::{
        build_mosaic, index::load_tiles_from_geoparquet, BBox, MosaicSpec, OutputWindow, Resample,
    };

    // Load tiles from GeoParquet (geometry=WKB, location=uri) with small sample.
    let tiles = load_tiles_from_geoparquet("tiles.parquet", "geometry", "location", None, Some(10))?;

    // Define mosaic spec (full extent).
    let spec = MosaicSpec {
        resx: 0.5,
        resy: 0.5,
        bbox: BBox::new(-180.0, -90.0, 180.0, 90.0),
        dst_crs: "EPSG:4326".into(),
        band_count: 3,
        data_type: mosaic_index::DataType::F32,
        blockxsize: 256,
        blockysize: 256,
        resampling: Resample::Bilinear,
        sort_ascending: true,
        output_nodata: f32::NAN,
        window: Some(OutputWindow::new(0, 0, 2048, 1024)),
    };

    let raster = build_mosaic(&spec, tiles, Default::default())?;
    println!(
        "materialized raster {}x{} bands={}",
        raster.width(),
        raster.height(),
        raster.bands()
    );
    Ok(())
}
