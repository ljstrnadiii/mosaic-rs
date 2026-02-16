use crate::planner::DestinationContext;
use crate::types::{MosaicSpec, TileRecord};
use geo::prelude::BoundingRect;

/// Filter tiles whose 4326 footprint bboxes intersect the destination AOI (also in 4326).
pub fn filter_and_sort_tiles(
    tiles: impl IntoIterator<Item = TileRecord>,
    spec: &MosaicSpec,
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
        (Some(sa), Some(sb)) => {
            let ord = sa.cmp(sb);
            if spec.sort_ascending {
                ord
            } else {
                ord.reverse()
            }
        }
        (Some(_), None) => {
            if spec.sort_ascending {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (None, Some(_)) => {
            if spec.sort_ascending {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        _ => a.location.cmp(&b.location),
    });

    filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BBox, DataType, MosaicSpec, SortValue, TileRecord};
    use geo::{LineString, MultiPolygon, Polygon};
    use warp_rs::Resample;

    fn square(minx: f64, miny: f64, maxx: f64, maxy: f64) -> MultiPolygon<f64> {
        let ring: LineString<f64> = vec![
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into();
        MultiPolygon(vec![Polygon::new(ring, vec![])])
    }

    fn base_spec(sort_ascending: bool) -> MosaicSpec {
        MosaicSpec {
            resx: 1.0,
            resy: 1.0,
            bbox: BBox::new(0.0, 0.0, 10.0, 10.0),
            dst_crs: "EPSG:4326".into(),
            band_count: 1,
            data_type: DataType::F32,
            blockxsize: 256,
            blockysize: 256,
            resampling: Resample::Nearest,
            sort_ascending,
            output_nodata: -9999.0,
            window: None,
        }
    }

    #[test]
    fn sort_respects_ascending() {
        let spec = base_spec(true);
        let dst = crate::planner::build_destination(&spec).expect("destination");
        let tiles = vec![
            TileRecord {
                location: "b".into(),
                footprint_4326: square(0.0, 0.0, 10.0, 10.0),
                sort_key: Some(SortValue::Int(2)),
            },
            TileRecord {
                location: "a".into(),
                footprint_4326: square(0.0, 0.0, 10.0, 10.0),
                sort_key: Some(SortValue::Int(1)),
            },
        ];

        let out = filter_and_sort_tiles(tiles, &spec, &dst);
        assert_eq!(out[0].location, "a");
        assert_eq!(out[1].location, "b");
    }

    #[test]
    fn sort_respects_descending() {
        let spec = base_spec(false);
        let dst = crate::planner::build_destination(&spec).expect("destination");
        let tiles = vec![
            TileRecord {
                location: "a".into(),
                footprint_4326: square(0.0, 0.0, 10.0, 10.0),
                sort_key: Some(SortValue::Int(1)),
            },
            TileRecord {
                location: "b".into(),
                footprint_4326: square(0.0, 0.0, 10.0, 10.0),
                sort_key: Some(SortValue::Int(2)),
            },
        ];

        let out = filter_and_sort_tiles(tiles, &spec, &dst);
        assert_eq!(out[0].location, "b");
        assert_eq!(out[1].location, "a");
    }
}
