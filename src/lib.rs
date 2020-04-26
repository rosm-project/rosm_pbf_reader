//! A low-level library for parsing OSM data in PBF format.
//!
//! The library provides [`PbfReader`](struct.PbfReader.html) for reading the blocks in an OSM PBF
//! and other utilities for reading densely or delta encoded data in these blocks.

use flate2::read::ZlibDecoder;
use quick_protobuf::{BytesReader, MessageRead};

mod proto;
use proto::fileformat::{BlobHeader, Blob};
use proto::osmformat::{DenseNodes, StringTable, Info};

use std::io::prelude::*;
use std::io::ErrorKind;
use std::str;
use std::str::Utf8Error;
use std::convert::From;

pub mod util;

#[doc(hidden)]
pub use proto::osmformat as pbf;

/// OpenStreetMap PBF reader.
///
/// An OSM PBF file is a sequence of blobs. These blobs can contain a 
/// [`HeaderBlock`](type.HeaderBlock.html) (OSMHeader),
/// a [`PrimitiveBlock`](type.PrimitiveBlock.html) (OSMData) or unknown data, in compressed or raw 
/// form. `PbfReader` parses these blobs and returns them through its 
/// [`read_block`](struct.PbfReader.html#method.read_block) method. 
/// 
/// # Links
///
/// - [Official OSM PBF format documentation](https://wiki.openstreetmap.org/wiki/PBF_Format).
pub struct PbfReader<Input> {
    pbf: Input,
    read_buffer: Vec<u8>, // Buffer used to read blob headers and blobs into
    block_data: Vec<u8>, // Buffer for currently read block
    can_continue: bool,
}

/// Possible errors returned by [`PbfReader::read_block`](struct.PbfReader.html#method.read_block).
#[derive(Debug)]
pub enum BlobReadError {
    /// Returned when a PBF parse error has occured.
    PbfParseError(quick_protobuf::Error),
    /// Returned when reading from the input stream or decompression of blob data has failed.
    IoError(std::io::Error),
    /// Returned when a blob header >= 64 KB is encountered.
    TooBigHeader,
    /// Returned when blob data >= 32 MB is encountered.
    TooBigData,
    /// Returned when an LZMA compressed blob is encountered.
    UnsupportedCompression,
    /// Returned when blob data is empty.
    NoData,
}

/// Result of [`PbfReader::read_block`](struct.PbfReader.html#method.read_block).
pub enum Block<'a> {
    /// A raw `OSMHeader` block.
    Header(pbf::HeaderBlock<'a>),
    /// A raw `OSMData` (primitive) block.
    Primitive(pbf::PrimitiveBlock<'a>),
    /// An unknown block.
    Unknown(&'a [u8]),
}

enum BlockType {
    Header, 
    Primitive,
    Unknown,
}

impl From<&str> for BlockType {
    fn from(value: &str) -> Self {
        match value {
            "OSMHeader" => BlockType::Header,
            "OSMData" => BlockType::Primitive,
            _ => BlockType::Unknown,
        }
    }
}

enum BlobReadResult {
    Block(BlockType),
    Error(BlobReadError),
    EndOfFile,
}

impl<Input> PbfReader<Input> where Input: std::io::Read {
    /// Constructs a new PBF reader.
    ///
    /// # Examples
    ///
    /// ```
    /// let file = File::open("some.osm.pbf").unwrap();
    ///
    /// let mut reader = PbfReader::new(file);
    ///
    /// while let Some(result) = reader.read_block() {
    ///     match result {
    ///         Ok(Block::Header(block)) => process_header_block(block),
    ///         Ok(Block::Primitive(block)) => process_primitive_block(block),
    ///         Ok(_) => println!("Skipping unknown block"),
    ///         Err(error) => println!("Error during read: {:?}", error),
    ///     }
    /// }
    ///
    /// fn process_header_block(block: pbf::HeaderBlock) { ... }
    /// fn process_primitive_block(block: pbf::PrimitiveBlock) { ... }
    /// ```
    pub fn new(pbf: Input) -> Self {
        PbfReader {
            pbf,
            read_buffer: Vec::new(),
            block_data: Vec::new(),
            can_continue: true,
        }
    }

    fn read_blob(&mut self) -> BlobReadResult {
        // Read blob header size

        let mut header_size_buffer = [0u8; 4];

        if let Err(error) = self.pbf.read_exact(&mut header_size_buffer) {
            return match error.kind() {
                ErrorKind::UnexpectedEof => BlobReadResult::EndOfFile,
                _ => BlobReadResult::Error(BlobReadError::IoError(error)),
            };
        }

        let blob_header_size = i32::from_be_bytes(header_size_buffer) as usize;

        if blob_header_size >= 64 * 1024 {
            return BlobReadResult::Error(BlobReadError::TooBigHeader);
        }

        // Read blob header

        self.read_buffer.resize_with(blob_header_size, Default::default);
        if let Err(error) = self.pbf.read_exact(&mut self.read_buffer) {
            return BlobReadResult::Error(BlobReadError::IoError(error));
        }

        let mut header_reader = BytesReader::from_bytes(&self.read_buffer);
        let blob_header = match BlobHeader::from_reader(&mut header_reader, &self.read_buffer) {
            Ok(blob_header) => blob_header,
            Err(error) => return BlobReadResult::Error(BlobReadError::PbfParseError(error)),
        };

        let block_type = BlockType::from(blob_header.type_pb.as_ref());

        // Read blob data

        let blob_size = blob_header.datasize;

        if blob_size >= 32 * 1024 * 1024 {
            return BlobReadResult::Error(BlobReadError::TooBigData);
        }

        self.read_buffer.resize_with(blob_size as usize, Default::default);
        if let Err(error) = self.pbf.read_exact(&mut self.read_buffer) {
            return BlobReadResult::Error(BlobReadError::IoError(error));
        }

        let mut blob_reader = BytesReader::from_bytes(&self.read_buffer);
        let blob = match Blob::from_reader(&mut blob_reader, &self.read_buffer) {
            Ok(blob) => blob,
            Err(error) => return BlobReadResult::Error(BlobReadError::PbfParseError(error)),
        };

        if let Some(raw_data) = blob.raw {
            self.block_data.clone_from_slice(&raw_data); // TODO: avoid copy

            BlobReadResult::Block(block_type)
        } else if let Some(zlib_data) = blob.zlib_data {
            let uncompressed_size = blob.raw_size.unwrap();
            self.block_data.resize_with(uncompressed_size as usize, Default::default);

            let mut decoder = ZlibDecoder::new(zlib_data.as_ref());

            match decoder.read_exact(&mut self.block_data) {
                Ok(_) => BlobReadResult::Block(block_type),
                Err(error) => BlobReadResult::Error(BlobReadError::IoError(error)),
            }
        } else if blob.lzma_data.is_some() {
            BlobReadResult::Error(BlobReadError::UnsupportedCompression)
        } else {
            BlobReadResult::Error(BlobReadError::NoData)
        }
    }

    /// Reads the next block.
    pub fn read_block(&mut self) -> Option<Result<Block, BlobReadError>> {
        if !self.can_continue {
            return None;
        }

        match self.read_blob() {
            BlobReadResult::Block(block_type) => {
                let mut block_reader = BytesReader::from_bytes(&self.block_data);

                let result = match block_type {
                    BlockType::Header => {
                        match pbf::HeaderBlock::from_reader(&mut block_reader, &self.block_data) {
                            Ok(header_block) => Ok(Block::Header(header_block)),
                            Err(error) => Err(BlobReadError::PbfParseError(error)),
                        }
                    },
                    BlockType::Primitive => {
                        match pbf::PrimitiveBlock::from_reader(&mut block_reader, &self.block_data) {
                            Ok(primitive_block) => Ok(Block::Primitive(primitive_block)),
                            Err(error) => Err(BlobReadError::PbfParseError(error)),
                        }
                    },
                    BlockType::Unknown => {
                        Ok(Block::Unknown(&self.block_data))
                    },
                };

                Some(result)
            },
            BlobReadResult::Error(error) => {
                self.can_continue = false;
                Some(Err(error))
            },
            BlobReadResult::EndOfFile => { 
                self.can_continue = false;
                None
            }
        }
    }
}

/// Utility for reading tags of dense nodes.
///
/// See [`DenseNode::tags`](struct.DenseNode.html#structfield.tags).
pub struct DenseTagReader<'a> {
    string_table: &'a StringTable<'a>,
    indices_it: std::slice::Iter<'a, i32>,
}

impl<'a> Iterator for DenseTagReader<'a> {
    type Item = (Result<&'a str, Utf8Error>, Result<&'a str, Utf8Error>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.indices_it.next() {
            Some(key_index) => {
                let key = str::from_utf8(self.string_table.s[*key_index as usize].as_ref());

                let value_index = self.indices_it.next()?;
                let value = str::from_utf8(self.string_table.s[*value_index as usize].as_ref());

                Some((key, value))
            },
            None => None,
        }
    }
}

/// Utility for reading tags.
pub struct TagReader<'a> {
    string_table: &'a StringTable<'a>,
    key_indices: &'a [u32],
    value_indices: &'a [u32],
    idx: usize,
}

impl<'a> TagReader<'a> {
    /// Constructs a new `TagReader` from key and value index slices, and a corresponding string table.
    ///
    /// # Examples
    /// ```
    /// for group in &block.primitivegroup {
    ///     for way in &group.ways {
    ///         let tags = TagReader::new(&way.keys, &way.vals, &block.stringtable);
    ///         for (key, value) in tags {
    ///             println!("{}: {}", key.unwrap(), value.unwrap());
    ///         }
    ///     }
    /// }
    pub fn new(key_indices: &'a [u32], value_indices: &'a [u32], string_table: &'a StringTable<'a>) -> Self {
        TagReader {
            string_table,
            key_indices,
            value_indices,
            idx: 0,
        }
    }
}

impl<'a> Iterator for TagReader<'a> {
    type Item = (Result<&'a str, Utf8Error>, Result<&'a str, Utf8Error>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.key_indices.len() {
            let key = str::from_utf8(self.string_table.s[self.key_indices[self.idx] as usize].as_ref());
            let value = str::from_utf8(self.string_table.s[self.value_indices[self.idx] as usize].as_ref());

            self.idx += 1;

            Some((key, value))
        } else {
            None
        }
    }
}

/// An unpacked dense node, returned when iterating on [`DenseNodeReader`](struct.DenseNodeReader.html).
pub struct DenseNode<'a> {
    pub id: i64,
    pub lat: i64,
    pub lon: i64,
    pub tags: DenseTagReader<'a>,
    pub info: Option<Info>,
}

#[derive(Default)]
struct DeltaCodedValues {
    id: i64,
    lat: i64,
    lon: i64,
    timestamp: i64,
    changeset: i64,
    uid: i32,
    user_sid: i32,
}

/// Utility for reading delta-encoded dense nodes.
pub struct DenseNodeReader<'a> {
    data: &'a DenseNodes,
    string_table: &'a StringTable<'a>,
    data_idx: usize,
    key_value_idx: usize, // Starting index of the next node's keys/values
    current: DeltaCodedValues, // Current values of delta coded fields
}

impl<'a> DenseNodeReader<'a> {
    /// Constructs a new `DenseNodeReader` from a slice of nodes.
    ///
    /// # Examples
    ///
    /// ```
    /// for group in &block.primitivegroup {
    ///     if let Some(dense_nodes) = &group.dense {
    ///         let nodes = DenseNodeReader::new(&dense_nodes, &block.stringtable);
    ///         for node in nodes {
    ///             for (key, value) in node.tags {
    ///                 println!("{}: {}", key.unwrap(), value.unwrap());
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    pub fn new(data: &'a DenseNodes, string_table: &'a StringTable<'a>) -> Self {
        DenseNodeReader {
            data,
            string_table,
            data_idx: 0,
            key_value_idx: 0,
            current: DeltaCodedValues::default(),
        }
    }
}

impl<'a> Iterator for DenseNodeReader<'a> {
    type Item = DenseNode<'a>;

    fn next(&mut self) -> Option<DenseNode<'a>> {
        if self.data_idx < self.data.id.len() {
            self.current.id += self.data.id[self.data_idx];
            self.current.lat += self.data.lat[self.data_idx];
            self.current.lon += self.data.lon[self.data_idx];

            let info = match &self.data.denseinfo {
                Some(dense_info) => {
                    // FIXME: seems like these arrays are always filled - it's not clear from the documentation if they can be empty or not
                    self.current.timestamp += dense_info.timestamp[self.data_idx];
                    self.current.changeset += dense_info.changeset[self.data_idx];
                    self.current.uid += dense_info.uid[self.data_idx];
                    self.current.user_sid += dense_info.user_sid[self.data_idx];

                    Some(Info {
                        version: dense_info.version[self.data_idx],
                        timestamp: Some(self.current.timestamp),
                        changeset: Some(self.current.changeset),
                        uid: Some(self.current.uid),
                        user_sid: Some(self.current.user_sid as u32), // u32 in the non-dense Info, probably a bug in the specification
                        visible: dense_info.visible.get(self.data_idx).cloned(),
                    })
                },
                None => None
            };

            let key_value_start = self.key_value_idx;

            for j in (self.key_value_idx..self.data.keys_vals.len()).step_by(2) {
                if self.data.keys_vals[j] == 0 {
                    self.key_value_idx = j + 1;
                    break; // Node end
                }
            }

            let key_value_slice = &self.data.keys_vals[key_value_start..self.key_value_idx-1];
            assert!(key_value_slice.len() % 2 == 0);

            self.data_idx += 1;

            Some(DenseNode {
                id: self.current.id,
                lat: self.current.lat,
                lon: self.current.lon,
                tags: DenseTagReader {
                    string_table: self.string_table,
                    indices_it: key_value_slice.iter(),
                },
                info,
            })
        } else {
            None
        }
    }
}

/// Utility for reading delta-encoded values directly, like `Way.refs` and `Relation.memids`.
pub struct DeltaValueReader<'a, T> {
    remaining: &'a [T],
    accumulated: T,
}

impl<'a, T> DeltaValueReader<'a, T> where T: std::default::Default  {
    /// Constructs a new `DeltaValueReader` from a slice of values.
    ///
    /// # Examples
    ///
    /// ```
    /// for group in &block.primitivegroup {
    ///     for way in &group.ways {
    ///         let refs = DeltaValueReader::new(&way.refs);
    ///         for ref in refs {
    ///             println!("{}", ref);
    ///         }
    ///     }
    /// }
    /// ```
    pub fn new(values: &'a [T]) -> Self {
        DeltaValueReader {
            remaining: values,
            accumulated: T::default(),
        }
    }
}

impl<'a, T> Iterator for DeltaValueReader<'a, T> where T: std::ops::AddAssign + std::clone::Clone {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((first, elements)) = self.remaining.split_first() {
            self.accumulated += first.clone();
            self.remaining = elements;
            Some(self.accumulated.clone())
        } else {
            None
        }
    }
}
