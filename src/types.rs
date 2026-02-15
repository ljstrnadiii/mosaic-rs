use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use geo::MultiPolygon;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, GtiError>;

#[derive(Debug, Error)]
pub enum GtiError {
    #[error("invalid mosaic spec: {0}")]
    InvalidSpec(String),
    #[error("destination dimensions exceed usize/u32 limits")]
    DimensionOverflow,
    #[error("warp error: {0}")]
    Warp(#[from] warp_rs::WarpError),
    #[error("proj error: {0}")]
    Proj(#[from] proj::ProjError),
    #[error("proj create error: {0}")]
    ProjCreate(#[from] proj::ProjCreateError),
    #[error("async-tiff error: {0}")]
    AsyncTiff(#[from] async_tiff::error::AsyncTiffError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("index load error: {0}")]
    IndexLoad(String),
    #[error("not yet implemented: {0}")]
    Unimplemented(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBox {
    pub minx: f64,
    pub miny: f64,
    pub maxx: f64,
    pub maxy: f64,
}

impl BBox {
    pub const fn new(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Self {
        Self {
            minx,
            miny,
            maxx,
            maxy,
        }
    }

    pub fn width(&self) -> f64 {
        self.maxx - self.minx
    }

    pub fn height(&self) -> f64 {
        self.maxy - self.miny
    }

    pub fn validate(&self) -> Result<()> {
        if !self.minx.is_finite()
            || !self.miny.is_finite()
            || !self.maxx.is_finite()
            || !self.maxy.is_finite()
        {
            return Err(GtiError::InvalidSpec("bbox has non-finite values".into()));
        }
        if self.maxx <= self.minx || self.maxy <= self.miny {
            return Err(GtiError::InvalidSpec(
                "bbox must have max > min in both axes".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputWindow {
    pub x_off: u32,
    pub y_off: u32,
    pub width: u32,
    pub height: u32,
}

impl OutputWindow {
    pub const fn new(x_off: u32, y_off: u32, width: u32, height: u32) -> Self {
        Self {
            x_off,
            y_off,
            width,
            height,
        }
    }

    pub const fn end_x(&self) -> u32 {
        self.x_off + self.width
    }

    pub const fn end_y(&self) -> u32 {
        self.y_off + self.height
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    U8,
    U16,
    I16,
    U32,
    I32,
    F32,
    F64,
}

#[derive(Debug, Clone)]
pub enum SortValue {
    String(String),
    Int(i64),
    Float(f64),
    DateTime(DateTime<Utc>),
}

impl SortValue {
    fn discriminant(&self) -> u8 {
        match self {
            SortValue::String(_) => 0,
            SortValue::Float(_) => 1,
            SortValue::Int(_) => 2,
            SortValue::DateTime(_) => 3,
        }
    }
}

impl PartialEq for SortValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SortValue {}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortValue::String(a), SortValue::String(b)) => a.cmp(b),
            (SortValue::Float(a), SortValue::Float(b)) => a.total_cmp(b),
            (SortValue::Int(a), SortValue::Int(b)) => a.cmp(b),
            (SortValue::DateTime(a), SortValue::DateTime(b)) => a.cmp(b),
            _ => self.discriminant().cmp(&other.discriminant()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MosaicSpec {
    pub resx: f64,
    pub resy: f64,
    pub bbox: BBox,
    pub dst_crs: String,
    pub band_count: u16,
    pub data_type: DataType,
    pub blockxsize: u32,
    pub blockysize: u32,
    pub resampling: warp_rs::Resample,
    pub sort_ascending: bool,
    pub output_nodata: f32,
    pub window: Option<OutputWindow>,
}

impl MosaicSpec {
    pub fn validate(&self) -> Result<()> {
        self.bbox.validate()?;
        if self.resx <= 0.0 || self.resy <= 0.0 {
            return Err(GtiError::InvalidSpec(
                "resx/resy must be positive pixel sizes".into(),
            ));
        }
        if self.blockxsize == 0 || self.blockysize == 0 {
            return Err(GtiError::InvalidSpec(
                "blockxsize/blockysize must be positive".into(),
            ));
        }
        if self.band_count == 0 {
            return Err(GtiError::InvalidSpec("band_count must be > 0".into()));
        }
        if let Some(w) = self.window
            && (w.width == 0 || w.height == 0)
        {
            return Err(GtiError::InvalidSpec(
                "output window width/height must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TileRecord {
    pub location: String,
    pub footprint_4326: MultiPolygon<f64>,
    pub sort_key: Option<SortValue>,
}

#[derive(Clone)]
pub struct TileMeta {
    #[allow(dead_code)]
    pub width: usize,
    #[allow(dead_code)]
    pub height: usize,
    pub bands: usize,
    pub src_grid: warp_rs::GridSpec,
    pub dst_to_src: std::sync::Arc<dyn warp_rs::CoordinateTransform>,
    pub nodata: Option<f32>,
}

#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub tokio_handle: Option<tokio::runtime::Handle>,
    pub object_store: std::sync::Arc<dyn object_store::ObjectStore>,
    /// Max number of tiles to process concurrently.
    pub max_tile_concurrency: usize,
    /// Max number of work items (window fetch + decode + reproject) per tile to run concurrently.
    pub max_work_concurrency: usize,
    /// Optional cache configuration (if None, no caching).
    pub cache: Option<CacheConfig>,
    /// Optional cap on how many tiles (in sorted/z-order) are allowed to contribute.
    pub z_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Entries in the metadata cache (TileHandle)
    pub meta_capacity: usize,
    /// Entries in the pixel cache (windowed decode results)
    pub pixel_capacity: usize,
}
