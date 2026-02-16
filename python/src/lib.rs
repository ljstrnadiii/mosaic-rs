#![deny(clippy::undocumented_unsafe_blocks)]

use chrono::{DateTime, FixedOffset, Utc};
use geo::{LineString, MultiPolygon, Polygon};
use mosaic_index::{
    BBox, BuildOptions, CacheConfig, DataType, GtiError, MosaicSpec, OutputWindow, RasterOwned,
    Resample, SortValue, TileRecord, build_mosaic, build_mosaic_async,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::AnyObjectStore;
use std::os::raw::c_int;
use std::sync::{Mutex, OnceLock};
use tracing_subscriber::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_META_CACHE_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_PIXEL_CACHE_BYTES: usize = 2 * 1024 * 1024 * 1024;
static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();
static TRACE_GUARD: OnceLock<Mutex<Option<tracing_chrome::FlushGuard>>> = OnceLock::new();

#[pyclass(name = "BBox", module = "mosaic_index")]
#[derive(Clone, Copy, Debug)]
struct PyBBox {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

#[pymethods]
impl PyBBox {
    #[new]
    fn new(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Self {
        Self {
            minx,
            miny,
            maxx,
            maxy,
        }
    }

    #[getter]
    fn minx(&self) -> f64 {
        self.minx
    }

    #[getter]
    fn miny(&self) -> f64 {
        self.miny
    }

    #[getter]
    fn maxx(&self) -> f64 {
        self.maxx
    }

    #[getter]
    fn maxy(&self) -> f64 {
        self.maxy
    }
}

impl From<PyBBox> for BBox {
    fn from(value: PyBBox) -> Self {
        BBox::new(value.minx, value.miny, value.maxx, value.maxy)
    }
}

#[pyclass(name = "OutputWindow", module = "mosaic_index")]
#[derive(Clone, Copy, Debug)]
struct PyOutputWindow {
    x_off: u32,
    y_off: u32,
    width: u32,
    height: u32,
}

#[pymethods]
impl PyOutputWindow {
    #[new]
    fn new(x_off: u32, y_off: u32, width: u32, height: u32) -> Self {
        Self {
            x_off,
            y_off,
            width,
            height,
        }
    }

    #[getter]
    fn x_off(&self) -> u32 {
        self.x_off
    }

    #[getter]
    fn y_off(&self) -> u32 {
        self.y_off
    }

    #[getter]
    fn width(&self) -> u32 {
        self.width
    }

    #[getter]
    fn height(&self) -> u32 {
        self.height
    }
}

impl From<PyOutputWindow> for OutputWindow {
    fn from(value: PyOutputWindow) -> Self {
        OutputWindow::new(value.x_off, value.y_off, value.width, value.height)
    }
}

#[pyclass(name = "MosaicSpec", module = "mosaic_index")]
#[derive(Clone)]
struct PyMosaicSpec {
    inner: MosaicSpec,
}

#[pymethods]
impl PyMosaicSpec {
    #[new]
    #[pyo3(signature = (
        resx,
        resy,
        bbox,
        dst_crs,
        *,
        band_count=1,
        data_type="F32",
        blockxsize=1024,
        blockysize=1024,
        resampling="Nearest",
        sort_ascending=true,
        output_nodata=-9999.0,
        window=None
    ))]
    fn new(
        resx: f64,
        resy: f64,
        bbox: PyBBox,
        dst_crs: String,
        band_count: u16,
        data_type: &str,
        blockxsize: u32,
        blockysize: u32,
        resampling: &str,
        sort_ascending: bool,
        output_nodata: f32,
        window: Option<PyOutputWindow>,
    ) -> PyResult<Self> {
        let inner = MosaicSpec {
            resx,
            resy,
            bbox: bbox.into(),
            dst_crs,
            band_count,
            data_type: parse_data_type(data_type)?,
            blockxsize,
            blockysize,
            resampling: parse_resample(resampling)?,
            sort_ascending,
            output_nodata,
            window: window.map(Into::into),
        };

        inner.validate().map_err(to_py_err)?;
        Ok(Self { inner })
    }
}

#[pyclass(name = "TileRecord", module = "mosaic_index")]
#[derive(Clone)]
struct PyTileRecord {
    inner: TileRecord,
}

#[pymethods]
impl PyTileRecord {
    #[new]
    #[pyo3(signature = (location, minx, miny, maxx, maxy, *, sort_key=None))]
    fn new(
        location: String,
        minx: f64,
        miny: f64,
        maxx: f64,
        maxy: f64,
        sort_key: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let footprint_4326 = bbox_to_multipolygon(minx, miny, maxx, maxy)?;
        let sort_key = Python::attach(|py| parse_sort_key(sort_key.as_ref().map(|v| v.bind(py))))?;

        Ok(Self {
            inner: TileRecord {
                location,
                footprint_4326,
                sort_key,
            },
        })
    }

    #[getter]
    fn location(&self) -> &str {
        &self.inner.location
    }
}

#[pyclass(name = "Raster", module = "mosaic_index", frozen)]
struct PyRaster {
    width: usize,
    height: usize,
    bands: usize,
    data: Vec<f32>,
    shape: [isize; 3],
    strides: [isize; 3],
}

#[pymethods]
impl PyRaster {
    #[getter]
    fn width(&self) -> usize {
        self.width
    }

    #[getter]
    fn height(&self) -> usize {
        self.height
    }

    #[getter]
    fn bands(&self) -> usize {
        self.bands
    }

    fn data<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let height = slf.height;
        let width = slf.width;
        let bands = slf.bands;
        let np = py.import("numpy")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("copy", false)?;
        let arr = np.call_method("asarray", (slf,), Some(&kwargs))?;
        arr.call_method1("reshape", (height, width, bands))
    }

    #[getter]
    fn shape(&self) -> (isize, isize, isize) {
        (self.shape[0], self.shape[1], self.shape[2])
    }

    /// Implements Python buffer protocol for zero-copy `np.asarray(raster, copy=False)`.
    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let itemsize = std::mem::size_of::<f32>();

        // SAFETY: `view` is provided by Python buffer machinery and points to writable
        // `Py_buffer` memory for this callback. We publish pointers into `self` and then
        // incref `self` on `view.obj` so the backing memory remains valid while the view lives.
        unsafe {
            (*view).buf = slf.data.as_ptr() as *mut std::ffi::c_void;
            (*view).len = (slf.data.len() * itemsize) as isize;
            (*view).itemsize = itemsize as isize;
            (*view).readonly = 1;
            (*view).ndim = 3;
            (*view).format = if flags & ffi::PyBUF_FORMAT != 0 {
                c"f".as_ptr() as *mut std::ffi::c_char
            } else {
                std::ptr::null_mut()
            };
            (*view).shape = slf.shape.as_ptr() as *mut isize;
            (*view).strides = slf.strides.as_ptr() as *mut isize;
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();
            (*view).obj = slf.as_ptr();
            ffi::Py_INCREF((*view).obj);
        }

        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // No-op. Shape/strides live on self and Python handles decref.
    }
}

impl PyRaster {
    fn from_raster(raster: RasterOwned) -> Self {
        let width = raster.width();
        let height = raster.height();
        let bands = raster.bands();
        let itemsize = std::mem::size_of::<f32>();
        let shape = [height as isize, width as isize, bands as isize];
        let strides = [
            (width * bands * itemsize) as isize,
            (bands * itemsize) as isize,
            itemsize as isize,
        ];
        Self {
            width,
            height,
            bands,
            data: raster.into_inner(),
            shape,
            strides,
        }
    }
}

#[pyfunction(name = "build_mosaic")]
#[pyo3(signature = (
    spec,
    tiles,
    *,
    store,
    max_tile_concurrency=32,
    max_work_concurrency=16,
    cache_meta_max_bytes=None,
    cache_pixel_max_bytes=None,
    z_limit=None
))]
fn build_mosaic_py(
    py: Python<'_>,
    spec: PyMosaicSpec,
    tiles: Vec<PyTileRecord>,
    store: AnyObjectStore,
    max_tile_concurrency: usize,
    max_work_concurrency: usize,
    cache_meta_max_bytes: Option<usize>,
    cache_pixel_max_bytes: Option<usize>,
    z_limit: Option<usize>,
) -> PyResult<PyRaster> {
    let spec = spec.inner;
    let tiles = tiles.into_iter().map(|tile| tile.inner).collect::<Vec<_>>();
    let opts = build_options(
        store,
        max_tile_concurrency,
        max_work_concurrency,
        cache_meta_max_bytes,
        cache_pixel_max_bytes,
        z_limit,
    );

    let raster = py.detach(move || build_mosaic(&spec, tiles, opts).map_err(to_py_err))?;
    Ok(PyRaster::from_raster(raster))
}

#[pyfunction(name = "build_mosaic_async")]
#[pyo3(signature = (
    spec,
    tiles,
    *,
    store,
    max_tile_concurrency=32,
    max_work_concurrency=16,
    cache_meta_max_bytes=None,
    cache_pixel_max_bytes=None,
    z_limit=None
))]
fn build_mosaic_async_py<'py>(
    py: Python<'py>,
    spec: PyMosaicSpec,
    tiles: Vec<PyTileRecord>,
    store: AnyObjectStore,
    max_tile_concurrency: usize,
    max_work_concurrency: usize,
    cache_meta_max_bytes: Option<usize>,
    cache_pixel_max_bytes: Option<usize>,
    z_limit: Option<usize>,
) -> PyResult<Bound<'py, PyAny>> {
    let spec = spec.inner;
    let tiles = tiles.into_iter().map(|tile| tile.inner).collect::<Vec<_>>();
    let opts = build_options(
        store,
        max_tile_concurrency,
        max_work_concurrency,
        cache_meta_max_bytes,
        cache_pixel_max_bytes,
        z_limit,
    );

    future_into_py(py, async move {
        let raster = build_mosaic_async(&spec, tiles, opts)
            .await
            .map_err(to_py_err)?;
        Ok(PyRaster::from_raster(raster))
    })
}

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pyfunction(name = "init_tracing")]
#[pyo3(signature = (*, rust_log=None, perfetto_path=None, include_args=false))]
fn init_tracing_py(
    rust_log: Option<String>,
    perfetto_path: Option<String>,
    include_args: bool,
) -> PyResult<bool> {
    if TRACING_INITIALIZED.get().is_some() {
        if perfetto_path.is_some() {
            let has_active_guard = TRACE_GUARD
                .get()
                .and_then(|slot| slot.lock().ok().map(|guard| guard.is_some()))
                .unwrap_or(false);
            if !has_active_guard {
                return Err(PyRuntimeError::new_err(
                    "tracing is already initialized in this Python process without active \
                     Perfetto output. Restart the interpreter and set MOSAIC_PERFETTO_TRACE \
                     before first mosaic call.",
                ));
            }
        }
        return Ok(false);
    }

    let default_directives = if perfetto_path.is_some() {
        // Perfetto timelines are most useful when span-level tracing is enabled.
        "mosaic=trace,mosaic_index=trace,async_tiff=trace,info"
    } else {
        "info"
    };

    let env_filter = match rust_log {
        Some(ref directives) => {
            tracing_subscriber::EnvFilter::try_new(directives).map_err(|e| {
                PyValueError::new_err(format!("invalid rust_log directives '{directives}': {e}"))
            })?
        }
        None => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_directives)),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(false);

    if let Some(path) = perfetto_path {
        let writer = std::fs::File::create(&path).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "failed to create perfetto trace file '{path}': {e}"
            ))
        })?;
        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .writer(writer)
            .include_args(include_args)
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(chrome_layer);
        tracing::subscriber::set_global_default(subscriber).map_err(|e| {
            PyRuntimeError::new_err(format!("failed to initialize tracing subscriber: {e}"))
        })?;
        let _ = TRACE_GUARD.set(Mutex::new(Some(guard)));
    } else {
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer);
        tracing::subscriber::set_global_default(subscriber).map_err(|e| {
            PyRuntimeError::new_err(format!("failed to initialize tracing subscriber: {e}"))
        })?;
    }

    let _ = TRACING_INITIALIZED.set(());
    tracing::info!(
        target: "mosaic",
        perfetto_enabled = TRACE_GUARD.get().is_some(),
        "python tracing initialized"
    );
    Ok(true)
}

#[pyfunction(name = "flush_tracing")]
fn flush_tracing_py() -> bool {
    if let Some(guard) = TRACE_GUARD.get()
        && let Ok(guard) = guard.lock()
        && let Some(guard) = guard.as_ref()
    {
        guard.flush();
        return true;
    }
    false
}

#[pyfunction(name = "shutdown_tracing")]
fn shutdown_tracing_py() -> bool {
    if let Some(guard) = TRACE_GUARD.get()
        && let Ok(mut guard) = guard.lock()
    {
        let dropped = guard.take();
        drop(dropped);
        return true;
    }
    false
}

fn build_options(
    store: AnyObjectStore,
    max_tile_concurrency: usize,
    max_work_concurrency: usize,
    cache_meta_max_bytes: Option<usize>,
    cache_pixel_max_bytes: Option<usize>,
    z_limit: Option<usize>,
) -> BuildOptions {
    BuildOptions {
        tokio_handle: None,
        object_store: store.into_dyn(),
        max_tile_concurrency,
        max_work_concurrency,
        cache: build_cache_config(cache_meta_max_bytes, cache_pixel_max_bytes),
        z_limit,
        fetch_tiles_debug_log: None,
        perf_stats: None,
    }
}

fn build_cache_config(meta: Option<usize>, pixel: Option<usize>) -> Option<CacheConfig> {
    match (meta, pixel) {
        (None, None) => None,
        _ => Some(CacheConfig {
            meta_max_bytes: meta.unwrap_or(DEFAULT_META_CACHE_BYTES),
            pixel_max_bytes: pixel.unwrap_or(DEFAULT_PIXEL_CACHE_BYTES),
        }),
    }
}

fn bbox_to_multipolygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> PyResult<MultiPolygon<f64>> {
    if !minx.is_finite() || !miny.is_finite() || !maxx.is_finite() || !maxy.is_finite() {
        return Err(PyValueError::new_err("bbox has non-finite values"));
    }
    if maxx <= minx || maxy <= miny {
        return Err(PyValueError::new_err(
            "bbox must have max > min in both axes",
        ));
    }

    let exterior = LineString::from(vec![
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]);
    Ok(MultiPolygon(vec![Polygon::new(exterior, vec![])]))
}

fn parse_sort_key(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<SortValue>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }

    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "sort_key cannot be bool; use int, float, str, datetime, or None",
        ));
    }

    if let Ok(v) = value.extract::<i64>() {
        return Ok(Some(SortValue::Int(v)));
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(Some(SortValue::Float(v)));
    }
    if let Ok(v) = value.extract::<DateTime<Utc>>() {
        return Ok(Some(SortValue::DateTime(v)));
    }
    if let Ok(v) = value.extract::<DateTime<FixedOffset>>() {
        return Ok(Some(SortValue::DateTime(v.with_timezone(&Utc))));
    }
    if let Ok(v) = value.extract::<String>() {
        return Ok(Some(SortValue::String(v)));
    }

    Err(PyTypeError::new_err(
        "sort_key must be int, float, str, datetime, or None",
    ))
}

fn parse_data_type(name: &str) -> PyResult<DataType> {
    match name.to_ascii_lowercase().as_str() {
        "u8" => Ok(DataType::U8),
        "u16" => Ok(DataType::U16),
        "i16" => Ok(DataType::I16),
        "u32" => Ok(DataType::U32),
        "i32" => Ok(DataType::I32),
        "f32" => Ok(DataType::F32),
        "f64" => Ok(DataType::F64),
        _ => Err(PyValueError::new_err(format!(
            "unsupported data_type '{name}'. Expected one of: U8, U16, I16, U32, I32, F32, F64"
        ))),
    }
}

fn parse_resample(name: &str) -> PyResult<Resample> {
    match name.to_ascii_lowercase().as_str() {
        "nearest" => Ok(Resample::Nearest),
        "bilinear" => Ok(Resample::Bilinear),
        _ => Err(PyValueError::new_err(format!(
            "unsupported resampling '{name}'. Expected one of: Nearest, Bilinear"
        ))),
    }
}

fn to_py_err(err: GtiError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[pymodule]
fn _mosaic_index(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;
    m.add_wrapped(wrap_pyfunction!(init_tracing_py))?;
    m.add_wrapped(wrap_pyfunction!(flush_tracing_py))?;
    m.add_wrapped(wrap_pyfunction!(shutdown_tracing_py))?;
    m.add_wrapped(wrap_pyfunction!(build_mosaic_py))?;
    m.add_wrapped(wrap_pyfunction!(build_mosaic_async_py))?;

    m.add_class::<PyBBox>()?;
    m.add_class::<PyOutputWindow>()?;
    m.add_class::<PyMosaicSpec>()?;
    m.add_class::<PyTileRecord>()?;
    m.add_class::<PyRaster>()?;

    pyo3_object_store::register_exceptions_module(py, m, "mosaic_index", "exceptions")?;

    Ok(())
}
