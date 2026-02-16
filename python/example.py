#!/usr/bin/env python3
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import geopandas as gpd
import numpy as np
import rasterio
from obstore.store import S3Store
from rasterio.transform import from_origin

from mosaic_index import (
    BBox,
    MosaicSpec,
    TileRecord,
    TracingSession,
    build_mosaic,
)

# Hard-coded inputs, matching the e2e style.
INDEX_PATH = "../index.parquet"
OUTPUT_GEOTIFF_PATH = "/tmp/test_window_geotiff.tif"
URL_COLUMN = "url"
SORT_COLUMN = "time"
GEOMETRY_COLUMN = "geometry"
DST_CRS = "EPSG:4326"
AWS_REGION = "us-west-2"

# Colorado 1.2 degree box centered on 4 interseting tiles from two different CRSs (UTM zones).
width = .6
height = .6
BBOX = BBox(
    -107.333 - width / 2, 39.668 - height / 2, -107.333 + width / 2, 39.668 + height / 2
)

# Build settings similar to tests/e2e_window.rs.
RES_METERS = 10.0
BAND_COUNT = 5
BLOCK_SIZE = 1024
OUTPUT_NODATA = -9999.0

# options for profiling
RUST_LOG = os.getenv("RUST_LOG")
PERFETTO_TRACE_PATH = os.getenv("MOSAIC_PERFETTO_TRACE")

_logger = logging.getLogger("simple_mosaic")


def split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def meters_to_degrees(res_meters: float, center_lat: float) -> tuple[float, float]:
    meters_per_deg_lat = 111_000.0
    meters_per_deg_lon = meters_per_deg_lat * np.cos(np.radians(center_lat))
    resx = res_meters / meters_per_deg_lon
    resy = res_meters / meters_per_deg_lat
    return float(resx), float(resy)


def load_tiles() -> list[TileRecord]:
    _logger.info("Loading index parquet: %s", INDEX_PATH)
    gdf = gpd.read_parquet(INDEX_PATH)
    _logger.info("Loaded %d index rows", len(gdf))

    tiles: list[TileRecord] = [
        TileRecord(
            str(row[URL_COLUMN]), *row["geometry"].bounds, sort_key=row[SORT_COLUMN]
        )
        for _, row in gdf.iterrows()
    ]
    return tiles


def write_geotiff(data_chunky: np.ndarray, resx: float, resy: float) -> None:
    band_first = np.moveaxis(data_chunky, 2, 0)
    count, height, width = band_first.shape
    transform = from_origin(BBOX.minx, BBOX.maxy, resx, resy)
    out_path = str(Path(OUTPUT_GEOTIFF_PATH).expanduser().resolve())
    _logger.info("Writing output GeoTIFF to: %s", out_path)

    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=count,
        dtype="float32",
        crs=DST_CRS,
        transform=transform,
        nodata=OUTPUT_NODATA,
        compress="DEFLATE",
    ) as dst:
        dst.write(band_first)
    _logger.info("GeoTIFF write complete")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _logger.info("Starting simple mosaic example")

    center_lat = (BBOX.miny + BBOX.maxy) / 2.0
    resx, resy = meters_to_degrees(RES_METERS, center_lat)
    _logger.info("Resolution degrees: resx=%s resy=%s", resx, resy)

    tiles = load_tiles()
    bucket, _ = split_s3_uri(tiles[0].location)
    _logger.info("Using S3 bucket: %s", bucket)
    store = S3Store(bucket, region=AWS_REGION)

    spec = MosaicSpec(
        resx=resx,
        resy=resy,
        bbox=BBOX,
        dst_crs=DST_CRS,
        band_count=BAND_COUNT,
        data_type="F32",
        blockxsize=BLOCK_SIZE,
        blockysize=BLOCK_SIZE,
        resampling="Nearest",
        sort_ascending=True,
        output_nodata=OUTPUT_NODATA,
    )

    with TracingSession(rust_log=RUST_LOG, perfetto_path=PERFETTO_TRACE_PATH):
        raster = build_mosaic(
            spec,
            tiles,  # TODO: take anything that implements an geoarrow table to directly pass gdf
            store=store,
            max_tile_concurrency=32,
            max_work_concurrency=16,
            cache_meta_max_bytes=1 * 1024 * 1024 * 1024,
            cache_pixel_max_bytes=8 * 1024 * 1024 * 1024,
            z_limit=4,
        )

        _logger.info(
            "Mosaic complete: width=%d height=%d bands=%d",
            raster.width,
            raster.height,
            raster.bands,
        )

        _logger.info("Accessing mosaic data as numpy array zero-copy")
        array = raster.data()
        _logger.info("Data array shape: %s", array.shape)

        # TODO: attach spec to raster
        write_geotiff(array, resx=resx, resy=resy)

        _logger.info("wrote %s", OUTPUT_GEOTIFF_PATH)
        _logger.info("tiles_used=%d", len(tiles))
        _logger.info(
            "shape=(height=%d, width=%d, bands=%d)",
            raster.height,
            raster.width,
            raster.bands,
        )
    if PERFETTO_TRACE_PATH:
        _logger.info("perfetto trace finalized at %s", PERFETTO_TRACE_PATH)


if __name__ == "__main__":
    main()
