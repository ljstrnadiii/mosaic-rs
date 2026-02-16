use std::convert::TryFrom;

use warp_rs::RasterOwned;

use crate::types::{GtiError, OutputWindow, Result};

/// Allocate an output raster for the requested window, prefilled with nodata.
pub fn allocate_output(window: &OutputWindow, band_count: u16, nodata: f32) -> Result<RasterOwned> {
    let width = usize::try_from(window.width).map_err(|_| GtiError::DimensionOverflow)?;
    let height = usize::try_from(window.height).map_err(|_| GtiError::DimensionOverflow)?;
    let bands = usize::from(band_count);

    Ok(RasterOwned::from_filled(width, height, bands, nodata))
}

/// Copy a block raster into the destination at the given top-left offsets (window-relative).
///
/// Only writes into pixels that are currently nodata, so earlier z-order tiles keep precedence.
/// Returns true when this destination block has no remaining nodata after the write.
pub fn blit_block(
    src: &RasterOwned,
    dst: &mut RasterOwned,
    x_off: usize,
    y_off: usize,
    nodata: f32,
) -> bool {
    let bands = dst.bands();
    let src_data = src.data();
    let mut block_complete = true;
    for y in 0..src.height() {
        let dy = y_off + y;
        if dy >= dst.height() {
            continue;
        }
        for x in 0..src.width() {
            let dx = x_off + x;
            if dx >= dst.width() {
                continue;
            }
            for b in 0..bands {
                let dst_idx = dst.index(dx, dy, b);
                let src_idx = src.index(x, y, b);
                let src_val = src_data[src_idx];
                let current = dst.data()[dst_idx];
                if is_nodata(current, nodata) && !is_nodata(src_val, nodata) {
                    dst.data_mut()[dst_idx] = src_val;
                }
                if is_nodata(dst.data()[dst_idx], nodata) {
                    block_complete = false;
                }
            }
        }
    }
    block_complete
}

fn is_nodata(value: f32, nodata: f32) -> bool {
    // Treat NaN as nodata regardless of configured sentinel. Reprojection can
    // generate NaNs at tile/window edges even when output_nodata is finite.
    if value.is_nan() {
        return true;
    }
    if nodata.is_nan() {
        value.is_nan()
    } else {
        value == nodata
    }
}
