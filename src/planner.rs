use std::convert::TryFrom;

use warp_rs::{
    Affine2D, CoordinateTransform, GridSpec, Resample, WarpWorkTile, plan_reproject_work,
};

use crate::types::{BBox, GtiError, MosaicSpec, OutputWindow, Result};

/// Destination grid plus the requested output window (defaults to full extent).
#[derive(Clone)]
pub struct DestinationContext {
    pub grid: GridSpec,
    pub window: OutputWindow,
    #[allow(dead_code)]
    pub dst_bbox: BBox,
    pub dst_bbox_4326: Option<BBox>,
}

/// Build the destination grid and clamp/validate the requested window.
pub fn build_destination(spec: &MosaicSpec) -> Result<DestinationContext> {
    // Width/height computed from bbox and positive pixel sizes.
    let width = (spec.bbox.width() / spec.resx).ceil();
    let height = (spec.bbox.height() / spec.resy).ceil();

    if width <= 0.0 || height <= 0.0 {
        return Err(GtiError::InvalidSpec(
            "computed width/height must be positive".into(),
        ));
    }

    let width_usize = usize::try_from(width as u64).map_err(|_| GtiError::DimensionOverflow)?;
    let height_usize = usize::try_from(height as u64).map_err(|_| GtiError::DimensionOverflow)?;
    let width_u32 = u32::try_from(width as u64).map_err(|_| GtiError::DimensionOverflow)?;
    let height_u32 = u32::try_from(height as u64).map_err(|_| GtiError::DimensionOverflow)?;

    // Origin at bbox.minx/maxy (top-left); resy applied negative for north-up grids.
    let affine = Affine2D::new(
        spec.resx,
        0.0,
        spec.bbox.minx,
        0.0,
        -(spec.resy.abs()),
        spec.bbox.maxy,
    );

    let mut grid = GridSpec::new(width_usize, height_usize, affine).with_crs(spec.dst_crs.clone());

    // Determine output window; default is full grid.
    let window = if let Some(win) = spec.window {
        // Validate window within grid bounds.
        if win.end_x() as usize > grid.width || win.end_y() as usize > grid.height {
            return Err(GtiError::InvalidSpec(
                "output window exceeds destination grid bounds".into(),
            ));
        }
        win
    } else {
        OutputWindow::new(0, 0, width_u32, height_u32)
    };

    // Adjust grid if a window is specified: shift affine origin to the window start.
    if window.x_off != 0 || window.y_off != 0 {
        let x = window.x_off as f64;
        let y = window.y_off as f64;
        grid.affine.c = grid
            .affine
            .a
            .mul_add(x, grid.affine.b.mul_add(y, grid.affine.c));
        grid.affine.f = grid
            .affine
            .d
            .mul_add(x, grid.affine.e.mul_add(y, grid.affine.f));
        grid.width = window.width as usize;
        grid.height = window.height as usize;
    }

    let dst_bbox = window_bbox_dst(&grid);
    let dst_bbox_4326 = bbox_to_4326(&dst_bbox, &spec.dst_crs)?;

    Ok(DestinationContext {
        grid,
        window,
        dst_bbox,
        dst_bbox_4326,
    })
}

/// Work block description (row-major traversal).
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct WorkBlock {
    pub x: usize,
    pub y: usize,
    pub width: usize,
    pub height: usize,
}

/// Partition the requested output window into blocks using row-major order.
#[allow(dead_code)]
pub fn plan_blocks(window: &OutputWindow, blockx: u32, blocky: u32) -> Vec<WorkBlock> {
    let mut blocks = Vec::new();
    let blockx = blockx.max(1) as usize;
    let blocky = blocky.max(1) as usize;

    let width = window.width as usize;
    let height = window.height as usize;

    let mut y = 0usize;
    while y < height {
        let h = blocky.min(height - y);
        let mut x = 0usize;
        while x < width {
            let w = blockx.min(width - x);
            blocks.push(WorkBlock {
                x: x + window.x_off as usize,
                y: y + window.y_off as usize,
                width: w,
                height: h,
            });
            x += blockx;
        }
        y += blocky;
    }

    blocks
}

/// Build a destination sub-grid for a given work tile (offsetting affine).
pub fn block_subgrid(dst_grid: &GridSpec, work: &WarpWorkTile) -> GridSpec {
    let mut affine = dst_grid.affine;
    let dx = work.dst_x as f64;
    let dy = work.dst_y as f64;
    let new_c = affine.a.mul_add(dx, affine.b.mul_add(dy, affine.c));
    let new_f = affine.d.mul_add(dx, affine.e.mul_add(dy, affine.f));
    affine.c = new_c;
    affine.f = new_f;
    let mut grid = GridSpec::new(work.dst_width, work.dst_height, affine);
    grid.crs = dst_grid.crs.clone();
    grid
}

/// Source tile geometry + transform from destination world coords into the tile's world coords.
#[allow(dead_code)]
pub struct TileGeometry {
    pub src_grid: GridSpec,
    pub dst_to_src: std::sync::Arc<dyn CoordinateTransform>,
}

/// Planned work entry tying a tile to a destination work tile.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlannedTileWork {
    pub tile_idx: usize,
    pub work: WarpWorkTile,
}

/// Plan reproject work tiles for each source tile against the destination grid.
///
/// - Partitions the destination using `blockx/blocky`.
/// - Uses `plan_reproject_work` to derive per-tile source windows.
/// - Skips tiles/work where no source pixels are reachable (`src_window = None`).
#[allow(dead_code)]
pub fn plan_tile_reprojects(
    dst: &DestinationContext,
    tiles: &[TileGeometry],
    blockx: u32,
    blocky: u32,
    resample: Resample,
) -> Result<Vec<PlannedTileWork>> {
    let mut out = Vec::new();
    for (idx, tile) in tiles.iter().enumerate() {
        let works = plan_reproject_work(
            &tile.src_grid,
            &dst.grid,
            blockx as usize,
            blocky as usize,
            resample,
            tile.dst_to_src.as_ref(),
        )?;
        for work in works.into_iter().filter(|w| w.src_window.is_some()) {
            out.push(PlannedTileWork {
                tile_idx: idx,
                work,
            });
        }
    }
    Ok(out)
}

/// Compute the dst-space bbox for the current grid (after any window shift).
fn window_bbox_dst(grid: &GridSpec) -> BBox {
    let minx = grid.affine.c;
    let maxx = grid.affine.a.mul_add(grid.width as f64, minx);
    let y0 = grid.affine.f;
    let y1 = grid.affine.e.mul_add(grid.height as f64, y0);
    let (miny, maxy) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    BBox::new(minx, miny, maxx, maxy)
}

/// Attempt to transform a bbox into EPSG:4326 using proj (if feature enabled).
fn bbox_to_4326(bbox: &BBox, dst_crs: &str) -> Result<Option<BBox>> {
    #[cfg(feature = "proj")]
    {
        let proj = proj::Proj::new_known_crs(dst_crs, "EPSG:4326", None)?;
        let (minx, miny) = proj.convert((bbox.minx, bbox.miny))?;
        let (maxx, maxy) = proj.convert((bbox.maxx, bbox.maxy))?;
        let (minx, maxx) = if minx <= maxx {
            (minx, maxx)
        } else {
            (maxx, minx)
        };
        let (miny, maxy) = if miny <= maxy {
            (miny, maxy)
        } else {
            (maxy, miny)
        };
        Ok(Some(BBox::new(minx, miny, maxx, maxy)))
    }
    #[cfg(not(feature = "proj"))]
    {
        let _ = (bbox, dst_crs);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, MosaicSpec};

    fn base_spec() -> MosaicSpec {
        MosaicSpec {
            resx: 1.0,
            resy: 1.0,
            bbox: BBox::new(0.0, 0.0, 100.0, 100.0),
            dst_crs: "EPSG:4326".to_string(),
            band_count: 1,
            data_type: DataType::F32,
            blockxsize: 64,
            blockysize: 64,
            resampling: warp_rs::Resample::Nearest,
            sort_ascending: true,
            output_nodata: -9999.0,
            window: None,
        }
    }

    #[test]
    fn window_defaults_to_full_extent() {
        let spec = base_spec();
        let dst = build_destination(&spec).unwrap();
        assert_eq!(dst.window.x_off, 0);
        assert_eq!(dst.window.y_off, 0);
        assert_eq!(dst.window.width, 100);
        assert_eq!(dst.window.height, 100);
        assert_eq!(dst.grid.width, 100);
        assert_eq!(dst.grid.height, 100);
        assert!((dst.grid.affine.c - 0.0).abs() < 1e-6);
        assert!((dst.grid.affine.f - 100.0).abs() < 1e-6);
    }

    #[test]
    fn window_shift_updates_grid_origin() {
        let mut spec = base_spec();
        spec.window = Some(OutputWindow::new(10, 20, 16, 8));
        let dst = build_destination(&spec).unwrap();
        assert_eq!(dst.window.width, 16);
        assert_eq!(dst.window.height, 8);
        assert_eq!(dst.grid.width, 16);
        assert_eq!(dst.grid.height, 8);
        // Origin should shift from top-left by pixel offsets.
        assert!((dst.grid.affine.c - 10.0).abs() < 1e-6);
        assert!((dst.grid.affine.f - 80.0).abs() < 1e-6);
    }

    #[test]
    fn planner_builds_tile_work_for_identity_transform() {
        let spec = base_spec();
        let dst = build_destination(&spec).unwrap();

        let tile_geom = TileGeometry {
            src_grid: dst.grid.clone(),
            dst_to_src: std::sync::Arc::new(warp_rs::IdentityTransform),
        };

        let works = plan_tile_reprojects(
            &dst,
            &[tile_geom],
            spec.blockxsize,
            spec.blockysize,
            spec.resampling,
        )
        .unwrap();

        // Expect (100x100) / (64x64) rounded up → 4 work tiles for the single source tile.
        assert_eq!(works.len(), 4);
        assert!(works.iter().all(|w| w.work.src_window.is_some()));
    }
}
