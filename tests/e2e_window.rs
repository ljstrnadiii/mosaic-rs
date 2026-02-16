#![cfg(feature = "geoparquet")]

use mosaic_index::{
    BBox, BuildOptions, DataType, MosaicSpec, Resample, build_mosaic, load_tiles_from_geoparquet,
};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::fs::File;
use std::fs::read_to_string;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "perfetto")]
use tracing_subscriber::prelude::*;
use url::Url;
use warp_rs::GridSpec;

/// E2E smoke test: load a small subset of the repo-provided `index.parquet`,
/// build a small output window (1024x1024), and ensure the pipeline runs.
#[test]
fn build_small_window_from_geoparquet_subset() {
    let _perfetto_guard = init_tracing();

    // Load full index (assumed to fit in memory for test).
    let tiles = load_tiles_from_geoparquet("index.parquet", "geometry", "url", None, None)
        .expect("load tiles");
    assert!(!tiles.is_empty());

    // box around Denver, CO intersecting 4 tiles in the index in EPSG:4326.
    let center_lat = 39.7085_f64;
    let center_lon = -104.9402_f64;
    let half_width_deg = 0.6;
    let half_height_deg = 0.6;
    let bbox = BBox::new(
        center_lon - half_width_deg,
        center_lat - half_height_deg,
        center_lon + half_width_deg,
        center_lat + half_height_deg,
    );
    println!(
        "using bbox around Denver: min=({:.6},{:.6}) max=({:.6},{:.6}), tiles={}",
        bbox.minx,
        bbox.miny,
        bbox.maxx,
        bbox.maxy,
        tiles.len()
    );

    // Choose resolution so the window is ~1024 pixels wide/high.
    // Native S2 L2A resolution ~10 m; convert to degrees at this latitude.
    let meters_per_deg_lat = 111_000.0;
    let meters_per_deg_lon = meters_per_deg_lat * center_lat.to_radians().cos();
    let resx = 10.0 / meters_per_deg_lon; // degrees per 10 m east-west
    let resy = 10.0 / meters_per_deg_lat; // degrees per 10 m north-south

    let spec = MosaicSpec {
        resx,
        resy,
        bbox,
        dst_crs: "EPSG:4326".into(),
        band_count: 5,
        data_type: DataType::F32,
        blockxsize: 1024,
        blockysize: 1024,
        resampling: Resample::Nearest,
        sort_ascending: true,
        output_nodata: -9999.0,
        window: None,
    };

    let (bucket, _) = strip_s3(&tiles[0].location);
    // Avoid IMDS when creds are supplied via env.
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let Some((access_key, secret_key, session_token)) = aws_credentials_from_env_or_profile()
    else {
        eprintln!("skipping: no AWS credentials found in env or ~/.aws/credentials");
        return;
    };
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".into()))
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key);
    if let Some(token) = session_token {
        builder = builder.with_token(token);
    }
    let store = builder.build().expect("build s3 store");

    let opts = BuildOptions {
        tokio_handle: None,
        object_store: Arc::new(store),
        max_tile_concurrency: 8,
        max_work_concurrency: 8,
        cache: Some(mosaic_index::CacheConfig {
            meta_max_bytes: 1 * 1024 * 1024 * 1024,
            pixel_max_bytes: 5 * 1024 * 1024 * 1024,
        }),
        z_limit: None,
    };

    let result = build_mosaic(&spec, tiles, opts);
    let raster = result.expect("build_mosaic failed");
    let (valid_pixels, min_val, max_val) = band0_stats(&raster, spec.output_nodata);
    println!(
        "result stats: valid_pixels={}, min={}, max={}",
        valid_pixels, min_val, max_val
    );
    assert!(
        valid_pixels > 0,
        "output is all nodata for band 1; check bbox/transform inputs"
    );

    // Optionally write GeoTIFF for inspection in QGIS (set env WRITE_TIFF=1).
    if std::env::var("WRITE_TIFF").ok().as_deref() == Some("1") {
        let dst_grid = output_grid_from_spec(&spec, raster.width(), raster.height());
        let out_path = "/tmp/test_window.tif";
        write_geotiff_f32(out_path, &raster, &dst_grid, spec.output_nodata).expect("write geotiff");
        println!("wrote {} for inspection", out_path);
    }
}

#[cfg(feature = "perfetto")]
fn init_tracing() -> Option<tracing_chrome::FlushGuard> {
    let env_filter = tracing_subscriber::EnvFilter::from_default_env();
    let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer();
    if let Ok(path) = env::var("PERFETTO_TRACE") {
        let trace_path = if path.trim().is_empty() {
            "/tmp/mosaic.perfetto.json".to_string()
        } else {
            path
        };
        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .file(trace_path.clone())
            .include_args(true)
            .build();
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(chrome_layer)
            .try_init();
        eprintln!("perfetto trace enabled: {}", trace_path);
        Some(guard)
    } else {
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init();
        None
    }
}

#[cfg(not(feature = "perfetto"))]
fn init_tracing() -> Option<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    None
}

fn aws_credentials_from_env_or_profile() -> Option<(String, String, Option<String>)> {
    if let (Ok(access_key), Ok(secret_key)) = (
        env::var("AWS_ACCESS_KEY_ID"),
        env::var("AWS_SECRET_ACCESS_KEY"),
    ) {
        return Some((access_key, secret_key, env::var("AWS_SESSION_TOKEN").ok()));
    }

    let profile = env::var("AWS_PROFILE")
        .or_else(|_| env::var("AWS_DEFAULT_PROFILE"))
        .unwrap_or_else(|_| "default".to_string());
    credentials_from_shared_file(&profile)
}

fn credentials_from_shared_file(profile: &str) -> Option<(String, String, Option<String>)> {
    let home = env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".aws").join("credentials");
    let contents = read_to_string(path).ok()?;

    let section_header = format!("[{profile}]");
    let mut in_section = false;
    let mut access_key: Option<String> = None;
    let mut secret_key: Option<String> = None;
    let mut session_token: Option<String> = None;

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == section_header;
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let value = v.trim().to_string();
        match key {
            "aws_access_key_id" => access_key = Some(value),
            "aws_secret_access_key" => secret_key = Some(value),
            "aws_session_token" => session_token = Some(value),
            _ => {}
        }
    }

    Some((access_key?, secret_key?, session_token))
}

fn strip_s3(uri: &str) -> (String, String) {
    if let Ok(url) = Url::parse(uri) {
        let bucket = url.host_str().unwrap_or("").to_string();
        let key = url.path().trim_start_matches('/').to_string();
        (bucket, key)
    } else {
        (String::new(), uri.to_string())
    }
}

fn output_grid_from_spec(spec: &MosaicSpec, width: usize, height: usize) -> GridSpec {
    let mut affine = warp_rs::Affine2D::new(
        spec.resx,
        0.0,
        spec.bbox.minx,
        0.0,
        -(spec.resy.abs()),
        spec.bbox.maxy,
    );

    if let Some(window) = spec.window {
        let x = window.x_off as f64;
        let y = window.y_off as f64;
        affine.c = affine.a.mul_add(x, affine.b.mul_add(y, affine.c));
        affine.f = affine.d.mul_add(x, affine.e.mul_add(y, affine.f));
    }

    GridSpec::new(width, height, affine).with_crs(spec.dst_crs.clone())
}

fn band0_stats(raster: &mosaic_index::RasterOwned, nodata: f32) -> (usize, f32, f32) {
    let mut valid = 0usize;
    let mut min_val = f32::INFINITY;
    let mut max_val = f32::NEG_INFINITY;
    for y in 0..raster.height() {
        for x in 0..raster.width() {
            let v = raster.data()[raster.index(x, y, 0)];
            if is_nodata(v, nodata) {
                continue;
            }
            valid += 1;
            min_val = min_val.min(v);
            max_val = max_val.max(v);
        }
    }
    if valid == 0 {
        (0, f32::NAN, f32::NAN)
    } else {
        (valid, min_val, max_val)
    }
}

fn is_nodata(value: f32, nodata: f32) -> bool {
    if value.is_nan() {
        return true;
    }
    if nodata.is_nan() {
        value.is_nan()
    } else {
        value == nodata
    }
}

fn write_geotiff_f32(
    path: &str,
    raster: &mosaic_index::RasterOwned,
    dst_grid: &GridSpec,
    nodata: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    // Adapted from warp-rs example writer: classic TIFF, single strip, with GeoKeys.
    let width = u32::try_from(raster.width())?;
    let height = u32::try_from(raster.height())?;
    let bands = u32::try_from(raster.bands())?;
    let pixels = raster.data();
    let bits_per_sample = 32u16;
    let sample_format = 3u16; // IEEEFP
    let photometric = if bands == 1 { 1u32 } else { 2u32 };

    let epsg = parse_epsg_code(dst_grid.crs.as_deref()).ok_or("dst_crs must be EPSG:<code>")?;
    let geokeys = build_geo_key_directory(epsg);
    let model_transform = affine_to_model_transformation(dst_grid.affine);

    // IFD offset immediately after pixel data.
    let data_offset = 8u32;
    let data_bytes = u32::try_from(std::mem::size_of_val(pixels)).map_err(|_| "too big")?;
    let ifd_offset = data_offset + data_bytes;

    // Build IFD entries
    let mut entries = vec![
        IfdEntry::new(256, 4, 1, width),
        IfdEntry::new(257, 4, 1, height),
        IfdEntry::new(258, 3, bands, 8), // BitsPerSample offset patched later if bands > 1
        IfdEntry::new(259, 3, 1, 1),     // Compression=None
        IfdEntry::new(262, 3, 1, photometric),
        IfdEntry::new(273, 4, 1, data_offset),
        IfdEntry::new(274, 3, 1, 1),     // Orientation=TopLeft
        IfdEntry::new(277, 3, 1, bands), // SamplesPerPixel
        IfdEntry::new(278, 4, 1, height),
        IfdEntry::new(279, 4, 1, data_bytes),
        IfdEntry::new(284, 3, 1, 1),     // PlanarConfiguration=Chunky
        IfdEntry::new(339, 3, bands, 0), // SampleFormat (offset later)
        IfdEntry::new(34264, 12, 16, 0), // ModelTransformationTag
        IfdEntry::new(34735, 3, geokeys.len() as u32, 0), // GeoKeyDirectoryTag
        IfdEntry::new(33550, 12, 3, 0),  // ModelPixelScaleTag
        IfdEntry::new(33922, 12, 6, 0),  // ModelTiepointTag
        IfdEntry::new(42112, 2, nodata.to_string().len() as u32 + 1, 0), // GDAL_NODATA
    ];

    entries.sort_by_key(|e| e.tag);
    let entry_count = u16::try_from(entries.len()).map_err(|_| "too many IFD entries")?;
    let ifd_size = 2usize + usize::from(entry_count) * 12 + 4;
    let mut extra = Vec::<u8>::new();
    let mut push_extra = |bytes: &[u8], align: usize| -> Result<u32, Box<dyn std::error::Error>> {
        while align > 1 && !extra.len().is_multiple_of(align) {
            extra.push(0);
        }
        let absolute = ifd_offset
            .checked_add(ifd_size as u32)
            .and_then(|v| v.checked_add(extra.len() as u32))
            .ok_or_else(|| "TIFF extra offset overflow".to_string())?;
        extra.extend_from_slice(bytes);
        Ok(absolute)
    };

    // BitsPerSample array
    let mut bits_bytes = Vec::with_capacity((bands as usize) * 2);
    for _ in 0..bands {
        bits_bytes.extend_from_slice(&bits_per_sample.to_le_bytes());
    }
    let bits_offset = push_extra(&bits_bytes, 2)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 258)
        .unwrap()
        .value_or_offset = bits_offset;

    // SampleFormat array
    let mut sf_bytes = Vec::with_capacity((bands as usize) * 2);
    for _ in 0..bands {
        sf_bytes.extend_from_slice(&sample_format.to_le_bytes());
    }
    let sf_offset = push_extra(&sf_bytes, 2)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 339)
        .unwrap()
        .value_or_offset = sf_offset;

    // ModelTransformationTag
    let mut mt_bytes = Vec::with_capacity(16 * 8);
    for v in model_transform {
        mt_bytes.extend_from_slice(&v.to_le_bytes());
    }
    let mt_offset = push_extra(&mt_bytes, 8)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 34264)
        .unwrap()
        .value_or_offset = mt_offset;

    // GeoKeyDirectoryTag
    let mut gk_bytes = Vec::with_capacity(geokeys.len() * 2);
    for k in geokeys {
        gk_bytes.extend_from_slice(&k.to_le_bytes());
    }
    let gk_offset = push_extra(&gk_bytes, 2)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 34735)
        .unwrap()
        .value_or_offset = gk_offset;

    // ModelPixelScaleTag
    let scale = [dst_grid.affine.a, -dst_grid.affine.e, 0.0f64];
    let mut scale_bytes = Vec::with_capacity(3 * 8);
    for v in scale {
        scale_bytes.extend_from_slice(&v.to_le_bytes());
    }
    let scale_offset = push_extra(&scale_bytes, 8)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 33550)
        .unwrap()
        .value_or_offset = scale_offset;

    // ModelTiepointTag (tie pixel 0,0 to geotransform origin)
    let tie = [0.0f64, 0.0, 0.0, dst_grid.affine.c, dst_grid.affine.f, 0.0];
    let mut tie_bytes = Vec::with_capacity(6 * 8);
    for v in tie {
        tie_bytes.extend_from_slice(&v.to_le_bytes());
    }
    let tie_offset = push_extra(&tie_bytes, 8)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 33922)
        .unwrap()
        .value_or_offset = tie_offset;

    // GDAL_NODATA
    let nodata_str = nodata.to_string();
    let mut nodata_bytes = nodata_str.into_bytes();
    nodata_bytes.push(0);
    let nodata_offset = push_extra(&nodata_bytes, 1)?;
    entries
        .iter_mut()
        .find(|e| e.tag == 42112)
        .unwrap()
        .value_or_offset = nodata_offset;

    // Write file: header, data, IFD, extra
    let mut writer = std::io::BufWriter::new(File::create(path)?);
    writer.write_all(b"II")?;
    write_u16_le(&mut writer, 42)?;
    write_u32_le(&mut writer, ifd_offset)?;
    // Pixel data
    let mut buf = Vec::with_capacity(pixels.len() * 4);
    for v in pixels {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    writer.write_all(&buf)?;
    // IFD
    write_u16_le(&mut writer, entry_count)?;
    for e in &entries {
        write_ifd_entry(&mut writer, e)?;
    }
    write_u32_le(&mut writer, 0)?; // next IFD = 0
    writer.write_all(&extra)?;
    writer.flush()?;
    Ok(())
}

#[derive(Clone, Copy)]
struct IfdEntry {
    tag: u16,
    field_type: u16,
    count: u32,
    value_or_offset: u32,
}

impl IfdEntry {
    fn new(tag: u16, field_type: u16, count: u32, value_or_offset: u32) -> Self {
        Self {
            tag,
            field_type,
            count,
            value_or_offset,
        }
    }
}

fn parse_epsg_code(crs: Option<&str>) -> Option<u16> {
    let crs = crs?;
    let code = crs.strip_prefix("EPSG:")?;
    code.parse::<u16>().ok()
}

fn build_geo_key_directory(epsg: u16) -> Vec<u16> {
    let is_geographic = (4000..5000).contains(&epsg);
    if is_geographic {
        vec![
            1, 1, 0, 3, // header
            1024, 0, 1, 2, // GTModelTypeGeoKey = Geographic
            1025, 0, 1, 1, // GTRasterTypeGeoKey = PixelIsArea
            2048, 0, 1, epsg, // GeographicTypeGeoKey
        ]
    } else {
        vec![
            1, 1, 0, 3, // header
            1024, 0, 1, 1, // GTModelTypeGeoKey = Projected
            1025, 0, 1, 1, // GTRasterTypeGeoKey = PixelIsArea
            3072, 0, 1, epsg, // ProjectedCSTypeGeoKey
        ]
    }
}

fn affine_to_model_transformation(affine: warp_rs::Affine2D) -> [f64; 16] {
    [
        affine.a, affine.b, 0.0, affine.c, affine.d, affine.e, 0.0, affine.f, 0.0, 0.0, 1.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    ]
}

fn write_ifd_entry<W: Write>(writer: &mut W, entry: &IfdEntry) -> std::io::Result<()> {
    write_u16_le(writer, entry.tag)?;
    write_u16_le(writer, entry.field_type)?;
    write_u32_le(writer, entry.count)?;
    write_u32_le(writer, entry.value_or_offset)?;
    Ok(())
}

fn write_u16_le<W: Write>(writer: &mut W, value: u16) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn write_u32_le<W: Write>(writer: &mut W, value: u32) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}
