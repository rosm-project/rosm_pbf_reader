//! Helpers for reading dense nodes.

use crate::{pbf, Error, TagReader};

use std::iter::{Enumerate, Zip};
use std::ops::AddAssign;
use std::slice::Iter;

/// An unpacked dense node, returned when iterating on [`DenseNodeReader`].
pub struct DenseNode<'a> {
    pub id: i64,

    /// Latitude of the node in an encoded format.
    /// Use [`util::normalize_coord`] to convert it to nanodegrees.
    pub lat: i64,

    /// Longitude of the node in an encoded format.
    /// Use [`util::normalize_coord`] to convert it to nanodegrees.
    pub lon: i64,

    /// Optional metadata.
    pub info: Option<pbf::Info>,

    /// Key/value index slice of [`pbf::DenseNodes::keys_vals`]. Indices point into a [`pbf::StringTable`].
    /// Use [`DenseTagReader`] to read these key/value pairs conveniently.
    pub key_value_indices: &'a [i32],
}

#[derive(Default)]
struct DeltaCodedValues {
    id: i64,
    lat: i64,
    lon: i64,
    timestamp: i64,
    changeset: i64,
    uid: i32,
    user_sid: u32,
}

/// Utility for reading delta-encoded dense nodes.
pub struct DenseNodeReader<'a> {
    data: &'a pbf::DenseNodes,
    data_it: Enumerate<Zip<Iter<'a, i64>, Zip<Iter<'a, i64>, Iter<'a, i64>>>>, // (data_idx, (id_delta, (lat_delta, lon_delta))) iterator
    key_value_idx: usize,      // Starting index of the next node's keys/values
    current: DeltaCodedValues, // Current values of delta coded fields
}

impl<'a> DenseNodeReader<'a> {
    /// Constructs a new `DenseNodeReader` from a slice of nodes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rosm_pbf_reader::{pbf, Error};
    /// use rosm_pbf_reader::dense::{new_dense_tag_reader, DenseNodeReader};
    ///
    /// fn process_primitive_block(block: pbf::PrimitiveBlock) -> Result<(), Error> {
    ///     for group in &block.primitivegroup {
    ///         if let Some(dense_nodes) = &group.dense {
    ///             let nodes = DenseNodeReader::new(&dense_nodes)?;
    ///             for node in nodes {
    ///                 let tags = new_dense_tag_reader(&block.stringtable, node?.key_value_indices);
    ///                 for (key, value) in tags {
    ///                     println!("{}: {}", key?, value?);
    ///                 }
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new(data: &'a pbf::DenseNodes) -> Result<Self, Error> {
        if data.lat.len() != data.id.len() || data.lon.len() != data.id.len() {
            Err(Error::LogicError(format!(
                "dense node id/lat/lon counts differ: {}/{}/{}",
                data.id.len(),
                data.lat.len(),
                data.lon.len()
            )))
        } else {
            let data_it = data.id.iter().zip(data.lat.iter().zip(data.lon.iter())).enumerate();

            Ok(DenseNodeReader {
                data,
                data_it,
                key_value_idx: 0,
                current: DeltaCodedValues::default(),
            })
        }
    }
}

fn delta_decode<T>(current: &mut T, delta: Option<&T>) -> Option<T>
where
    T: AddAssign<T> + Copy,
{
    match delta {
        Some(delta) => {
            *current += *delta;
            Some(*current)
        }
        None => None,
    }
}

impl<'a> Iterator for DenseNodeReader<'a> {
    type Item = Result<DenseNode<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((data_idx, (id_delta, (lat_delta, lon_delta)))) = self.data_it.next() {
            self.current.id += id_delta;
            self.current.lat += lat_delta;
            self.current.lon += lon_delta;

            let info = match &self.data.denseinfo {
                Some(dense_info) => {
                    let user_sid = match dense_info.user_sid.get(data_idx) {
                        Some(user_sid_delta) => {
                            if let Some(current_user_sid) = self.current.user_sid.checked_add_signed(*user_sid_delta) {
                                self.current.user_sid = current_user_sid;
                                Some(self.current.user_sid)
                            } else {
                                return Some(Err(Error::LogicError(format!(
                                    "delta decoding `user_sid` results in a negative integer: {}+{}",
                                    self.current.user_sid, user_sid_delta
                                ))));
                            }
                        }
                        None => None,
                    };

                    Some(pbf::Info {
                        version: dense_info.version.get(data_idx).cloned(),
                        timestamp: delta_decode(&mut self.current.timestamp, dense_info.timestamp.get(data_idx)),
                        changeset: delta_decode(&mut self.current.changeset, dense_info.changeset.get(data_idx)),
                        uid: delta_decode(&mut self.current.uid, dense_info.uid.get(data_idx)),
                        user_sid,
                        visible: dense_info.visible.get(data_idx).cloned(),
                    })
                }
                None => None,
            };

            let key_value_indices = if !self.data.keys_vals.is_empty() {
                let next_zero = &self.data.keys_vals[self.key_value_idx..]
                    .iter()
                    .enumerate()
                    .step_by(2)
                    .find(|(_, string_idx)| **string_idx == 0);

                let next_zero_idx = if let Some((next_zero_idx, _)) = next_zero {
                    self.key_value_idx + *next_zero_idx
                } else {
                    self.data.keys_vals.len()
                };

                let key_value_start = self.key_value_idx;
                self.key_value_idx = next_zero_idx + 1;

                &self.data.keys_vals[key_value_start..self.key_value_idx - 1]
            } else {
                &[]
            };

            Some(Ok(DenseNode {
                id: self.current.id,
                lat: self.current.lat,
                lon: self.current.lon,
                key_value_indices,
                info,
            }))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod dense_node_reader_tests {
    use super::*;

    #[test]
    fn valid_input() {
        let dense_info = pbf::DenseInfo {
            user_sid: vec![i32::MAX, 1],
            version: vec![2, 4],
            timestamp: vec![2, 1],
            changeset: vec![2, -1],
            uid: vec![5, -1],
            visible: vec![true, false],
        };

        let dense_nodes = pbf::DenseNodes {
            id: vec![2, -1],
            denseinfo: Some(dense_info),
            lat: vec![-3, 1],
            lon: vec![3, -1],
            keys_vals: vec![1, 2, 0, 3, 4, 0],
        };

        let reader = DenseNodeReader::new(&dense_nodes).expect("dense node reader should be created on valid data");
        let mut result: Vec<DenseNode> = reader.filter_map(|r| r.ok()).collect();

        assert_eq!(result.len(), 2);
        let first = &mut result[0];
        assert_eq!(first.id, 2);
        assert_eq!(first.lat, -3);
        assert_eq!(first.lon, 3);
        assert_eq!(first.key_value_indices, [1, 2]);
        let first_info = first.info.as_ref().unwrap();
        assert_eq!(first_info.uid, Some(5));
        assert_eq!(first_info.timestamp, Some(2));
        assert_eq!(first_info.version, Some(2));
        assert_eq!(first_info.changeset, Some(2));
        assert_eq!(first_info.visible, Some(true));
        assert_eq!(first_info.user_sid, Some(i32::MAX as u32));

        let second = &mut result[1];
        assert_eq!(second.id, 1);
        assert_eq!(second.lat, -2);
        assert_eq!(second.lon, 2);
        assert_eq!(second.key_value_indices, [3, 4]);
        let second_info = second.info.as_ref().unwrap();
        assert_eq!(second_info.uid, Some(4));
        assert_eq!(second_info.timestamp, Some(3));
        assert_eq!(second_info.version, Some(4));
        assert_eq!(second_info.changeset, Some(1));
        assert_eq!(second_info.visible, Some(false));
        assert_eq!(second_info.user_sid, Some(i32::MAX as u32 + 1));
    }

    #[test]
    fn invalid_required_data_lengths() {
        let dense_nodes = |id_count: usize, lat_count: usize, lon_count: usize| pbf::DenseNodes {
            id: vec![0; id_count],
            denseinfo: None,
            lat: vec![0; lat_count],
            lon: vec![0; lon_count],
            keys_vals: vec![],
        };

        assert!(DenseNodeReader::new(&dense_nodes(0, 0, 0)).is_ok());
        assert!(DenseNodeReader::new(&dense_nodes(1, 0, 0)).is_err());
        assert!(DenseNodeReader::new(&dense_nodes(0, 1, 0)).is_err());
        assert!(DenseNodeReader::new(&dense_nodes(0, 0, 1)).is_err());
    }

    #[test]
    fn invalid_user_sid() {
        let dense_info = pbf::DenseInfo {
            user_sid: vec![0, -1],
            ..Default::default()
        };

        let dense_nodes = pbf::DenseNodes {
            id: vec![0, 0],
            denseinfo: Some(dense_info),
            lat: vec![0, 0],
            lon: vec![0, 0],
            keys_vals: vec![],
        };

        let mut reader = DenseNodeReader::new(&dense_nodes).expect("dense node reader should be created on valid data");

        let next = reader.next();
        assert!(next.is_some());
        let next = reader.next();
        assert!(next.is_some());
        assert!(next.unwrap().is_err());
    }
}

/// Constructs a new `TagReader` from a dense key/value index slice, and a corresponding string table.
///
/// See [`DenseNodeReader::new`] and [`DenseNode::key_value_indices`].
pub fn new_dense_tag_reader<'a>(
    string_table: &'a pbf::StringTable,
    key_value_indices: &'a [i32],
) -> TagReader<'a, impl Iterator<Item = (Result<usize, Error>, Result<usize, Error>)> + 'a> {
    TagReader {
        string_table,
        iter: key_value_indices.chunks_exact(2).map(|s| {
            let convert_idx = |index: i32| -> Result<usize, Error> {
                if let Ok(index) = TryInto::<usize>::try_into(index) {
                    Ok(index)
                } else {
                    Err(Error::LogicError(format!("string table index {} is invalid", index)))
                }
            };

            (convert_idx(s[0]), convert_idx(s[1]))
        }),
    }
}
