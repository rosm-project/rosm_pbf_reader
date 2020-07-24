//! Various utilities, e.g. coordinate normalization.

use crate::proto::osmformat as pbf;

/// Normalizes `lat` and `lon` to nanodegrees and returns them in a `(latitude: i64, longitude: i64)` pair.
pub fn normalize_coord(lat: i64, lon: i64, block: &pbf::PrimitiveBlock) -> (i64, i64) {
    (
        lat * block.granularity as i64 + block.lat_offset,
        lon * block.granularity as i64 + block.lon_offset,
    )
}

/// Normalizes a timestamp coming from `pbf::Info` or `pbf::DenseInfo` to nanoseconds.
pub fn normalize_timestamp(timestamp: i64, block: &pbf::PrimitiveBlock) -> i64 {
    timestamp * block.date_granularity as i64
}
