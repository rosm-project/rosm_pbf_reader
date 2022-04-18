//! Various utilities, like timestamp and coordinate normalization.

use crate::pbf;

/// Normalizes `lat` and `lon` to nanodegrees and returns them in a `(latitude, longitude)` pair.
pub fn normalize_coord(lat: i64, lon: i64, block: &pbf::PrimitiveBlock) -> (i64, i64) {
    (
        lat * block.granularity() as i64 + block.lat_offset(),
        lon * block.granularity() as i64 + block.lon_offset(),
    )
}

/// Normalizes a timestamp coming from [`pbf::Info`] or [`pbf::DenseInfo`] to nanoseconds.
pub fn normalize_timestamp(timestamp: i64, block: &pbf::PrimitiveBlock) -> i64 {
    timestamp * block.date_granularity() as i64
}
