use crate::planner::DestinationContext;
use crate::types::{MosaicSpec, TileRecord};
use geo::prelude::BoundingRect;

/// Filter tiles whose 4326 footprint bboxes intersect the destination AOI (also in 4326).
pub fn filter_and_sort_tiles(
    tiles: impl IntoIterator<Item = TileRecord>,
    _spec: &MosaicSpec,
    dst: &DestinationContext,
) -> Vec<TileRecord> {
    let mut filtered: Vec<TileRecord> = tiles
        .into_iter()
        .filter(|t| match dst.dst_bbox_4326 {
            Some(ref bbox4326) => {
                if let Some(rect) = t.footprint_4326.bounding_rect() {
                    rect.max().x >= bbox4326.minx
                        && rect.min().x <= bbox4326.maxx
                        && rect.max().y >= bbox4326.miny
                        && rect.min().y <= bbox4326.maxy
                } else {
                    true
                }
            }
            None => true,
        })
        .collect();

    filtered.sort_by(|a, b| match (&a.sort_key, &b.sort_key) {
        (Some(sa), Some(sb)) => sa.cmp(sb),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    });

    filtered
}

#[cfg(feature = "geoparquet")]
pub mod geoparquet_loader {
    use crate::types::{GtiError, SortValue, TileRecord};
    use arrow2::array::{Array, BinaryArray, Utf8Array};
    use arrow2::datatypes::Schema;
    use arrow2::error::Error as ArrowError;
    use arrow2::io::parquet::read::{FileReader, infer_schema, read_metadata};
    use geo::{Geometry, MultiPolygon};
    use geozero::ToGeo;
    use geozero::wkb::Wkb;
    use std::fs::File;

    /// Load tiles from a GeoParquet file, decoding the geometry column (WKB) and location/url column.
    ///
    /// `row_limit` allows sampling a small subset for tests.
    pub fn load_tiles_from_geoparquet(
        path: &str,
        geom_col: &str,
        location_col: &str,
        sort_col: Option<&str>,
        row_limit: Option<usize>,
    ) -> Result<Vec<TileRecord>, GtiError> {
        let mut file = File::open(path).map_err(|e| GtiError::IndexLoad(e.to_string()))?;
        let metadata = read_metadata(&mut file).map_err(|e| GtiError::IndexLoad(e.to_string()))?;
        let schema = infer_schema(&metadata).map_err(|e| GtiError::IndexLoad(e.to_string()))?;

        let geom_idx = find_index(&schema, geom_col)?;
        let loc_idx = find_index(&schema, location_col)?;
        let sort_idx = sort_col.map(|c| find_index(&schema, c)).transpose()?;

        let mut reader = FileReader::new(
            file,
            metadata.row_groups,
            schema.clone(),
            None,
            row_limit,
            None,
        );

        let mut tiles = Vec::new();
        let mut remaining = row_limit.unwrap_or(usize::MAX);

        for chunk in reader.by_ref() {
            let chunk = chunk.map_err(|e: ArrowError| GtiError::IndexLoad(e.to_string()))?;
            if chunk.is_empty() {
                continue;
            }
            let arrays = chunk.arrays();
            let geom_arr = as_binary(arrays[geom_idx].as_ref())?;
            let loc_arr = as_utf8(arrays[loc_idx].as_ref())?;
            let sort_arr = sort_idx.and_then(|idx| arrays.get(idx).map(|a| a.as_ref()));

            for i in 0..chunk.len().min(remaining) {
                let wkb_bytes = geom_arr.value(i);
                let geom: Geometry<f64> = Wkb(wkb_bytes.to_vec())
                    .to_geo()
                    .map_err(|e| GtiError::IndexLoad(e.to_string()))?;
                let footprint_4326 = match geom {
                    Geometry::Polygon(p) => MultiPolygon(vec![p]),
                    Geometry::MultiPolygon(mp) => mp,
                    _ => continue,
                };
                let location = loc_arr.value(i).to_string();
                let sort_key = sort_arr.and_then(|col| sort_value_from_any(col, i));
                tiles.push(TileRecord {
                    location,
                    footprint_4326,
                    sort_key,
                });
            }
            remaining = remaining.saturating_sub(chunk.len());
            if remaining == 0 {
                break;
            }
        }

        Ok(tiles)
    }

    fn find_index(schema: &Schema, name: &str) -> Result<usize, GtiError> {
        schema
            .fields
            .iter()
            .position(|f| f.name == name)
            .ok_or_else(|| GtiError::IndexLoad(format!("column not found: {name}")))
    }

    fn as_binary(array: &dyn Array) -> Result<&BinaryArray<i32>, GtiError> {
        array
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .ok_or_else(|| GtiError::IndexLoad("geometry column not binary".into()))
    }

    fn as_utf8(array: &dyn Array) -> Result<&Utf8Array<i32>, GtiError> {
        array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| GtiError::IndexLoad("location column not utf8".into()))
    }

    fn sort_value_from_any(array: &dyn Array, idx: usize) -> Option<SortValue> {
        if let Some(arr) = array.as_any().downcast_ref::<Utf8Array<i32>>() {
            arr.get(idx).map(|s| SortValue::String(s.to_string()))
        } else if let Some(arr) = array.as_any().downcast_ref::<arrow2::array::Int64Array>() {
            arr.get(idx).map(SortValue::Int)
        } else if let Some(arr) = array.as_any().downcast_ref::<arrow2::array::Float64Array>() {
            arr.get(idx).map(SortValue::Float)
        } else {
            None
        }
    }
}

#[cfg(feature = "geoparquet")]
pub use geoparquet_loader::load_tiles_from_geoparquet;
