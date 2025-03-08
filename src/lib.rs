//! A low-level library for parsing OSM data in PBF format.
//!
//! An OSM PBF file is a sequence of blobs. These blobs can be read with [`read_blob`]. The
//! [`RawBlock`]s returned by `read_blob` can then be decompressed and parsed by
//! [`BlockParser::parse_block`], which returns a [`Block`], containing either a parsed
//! header/primitive block or an unknown block's binary data.
//!
//! The library also provides utilities for reading densely or delta encoded data in these blocks.
//!
//! Raw header and primitive block definitions (generated by [Prost](https://github.com/tokio-rs/prost)) are exported
//! through the `pbf` module.
//!
//! # Links
//!
//! - [OSM PBF format documentation](https://wiki.openstreetmap.org/wiki/PBF_Format)

#![forbid(unsafe_code)]

#[cfg(feature = "default")]
use flate2::read::ZlibDecoder;

use prost::Message;

use std::convert::From;
#[cfg(feature = "default")]
use std::io::prelude::*;
use std::io::ErrorKind;
use std::str;

pub mod dense;
pub mod pbf;
pub mod util;

/// Possible errors returned by the library.
#[derive(Debug)]
pub enum Error {
    /// Returned when a PBF parse error has occured.
    PbfParseError(prost::DecodeError),
    /// Returned when reading from the input stream or decompression of blob data has failed.
    IoError(std::io::Error),
    /// Returned when a blob header with an invalid size (negative or >=64 KB) is encountered.
    InvalidBlobHeader,
    /// Returned when blob data with an invalid size (negative or >=32 MB) is encountered.
    InvalidBlobData,
    /// Returned when an error has occured during blob decompression.
    DecompressionError(DecompressionError),
    /// Returned when some assumption in the data is violated (for example, an out of bounds index is encountered).
    LogicError(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

/// Result of [`BlockParser::parse_block`].
pub enum Block<'a> {
    /// A raw `OSMHeader` block.
    Header(pbf::HeaderBlock),
    /// A raw `OSMData` (primitive) block.
    Primitive(pbf::PrimitiveBlock),
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

/// An unparsed, possibly compressed block.
pub struct RawBlock {
    r#type: BlockType,
    data: Vec<u8>,
}

/// Reads the next blob from `pbf`.
///
/// # Examples
///
/// ```no_run
/// use rosm_pbf_reader::read_blob;
///
/// use std::fs::File;
///
/// let mut file = File::open("some.osm.pbf").unwrap();
///
/// while let Some(result) = read_blob(&mut file) {
///     match result {
///         Ok(raw_block) => {}
///         Err(error) => {}
///     }
/// }
/// ```
pub fn read_blob<Input>(pbf: &mut Input) -> Option<Result<RawBlock, Error>>
where
    Input: std::io::Read,
{
    let mut header_size_buffer = [0u8; 4];

    if let Err(error) = pbf.read_exact(&mut header_size_buffer) {
        return match error.kind() {
            ErrorKind::UnexpectedEof => None,
            _ => Some(Err(Error::IoError(error))),
        };
    }

    Some(read_blob_inner(pbf, header_size_buffer))
}

fn read_blob_inner<Input>(pbf: &mut Input, header_size_buffer: [u8; 4]) -> Result<RawBlock, Error>
where
    Input: std::io::Read,
{
    use pbf::BlobHeader;

    let blob_header_size: usize = i32::from_be_bytes(header_size_buffer)
        .try_into()
        .map_err(|_err| Error::InvalidBlobHeader)?;

    if blob_header_size >= 64 * 1024 {
        return Err(Error::InvalidBlobHeader);
    }

    let mut blob = vec![0u8; blob_header_size];
    if let Err(error) = pbf.read_exact(&mut blob) {
        return Err(Error::IoError(error));
    }

    let blob_header = match BlobHeader::decode(&*blob) {
        Ok(blob_header) => blob_header,
        Err(error) => return Err(Error::PbfParseError(error)),
    };

    let block_type = BlockType::from(blob_header.r#type.as_ref());
    let blob_size: usize = blob_header.datasize.try_into().map_err(|_err| Error::InvalidBlobData)?;

    if blob_size >= 32 * 1024 * 1024 {
        return Err(Error::InvalidBlobData);
    }

    blob.resize_with(blob_size, Default::default);

    if let Err(error) = pbf.read_exact(&mut blob) {
        return Err(Error::IoError(error));
    }

    let raw_block = RawBlock {
        r#type: block_type,
        data: blob,
    };

    Ok(raw_block)
}

/// Blob compression method.
pub enum CompressionMethod {
    /// LZ4
    Lz4,
    /// LZMA
    Lzma,
    /// ZLib
    Zlib,
    /// Zstandard
    Zstd,
}

/// Possible errors returned by [Decompressor] implementations.
#[derive(Debug)]
pub enum DecompressionError {
    /// The given compression method isn't supported by the decompressor.
    UnsupportedCompression,
    /// An internal error occured during decompression.
    InternalError(Box<dyn std::error::Error + Send + Sync>),
}

/// Trait for custom decompression support.
pub trait Decompressor {
    /// Decompresses `input` blob into the preallocated `output` slice.
    fn decompress(method: CompressionMethod, input: &[u8], output: &mut [u8]) -> Result<(), DecompressionError>;
}

/// The default blob decompressor.
///
/// Supports ZLib decompression if default features are enabled.
pub struct DefaultDecompressor;

impl Decompressor for DefaultDecompressor {
    #[cfg(feature = "default")]
    fn decompress(method: CompressionMethod, input: &[u8], output: &mut [u8]) -> Result<(), DecompressionError> {
        match method {
            CompressionMethod::Zlib => {
                let mut decoder = ZlibDecoder::new(input);

                match decoder.read_exact(output) {
                    Ok(_) => Ok(()),
                    Err(error) => Err(DecompressionError::InternalError(Box::new(error))),
                }
            }
            _ => Err(DecompressionError::UnsupportedCompression),
        }
    }

    #[cfg(not(feature = "default"))]
    fn decompress(_method: CompressionMethod, _input: &[u8], _output: &mut [u8]) -> Result<(), DecompressionError> {
        Err(DecompressionError::UnsupportedCompression)
    }
}

/// Parser with an internal buffer for `RawBlock`s.
///
/// When multiple threads are used to speed up parsing, it's recommended to use a single
/// `BlockParser` per thread (e.g. by making it thread local), so its internal buffer remains
/// alive, avoiding repeated memory allocations.
pub struct BlockParser<D: Decompressor = DefaultDecompressor> {
    block_buffer: Vec<u8>,
    decompressor: std::marker::PhantomData<D>,
}

impl Default for BlockParser {
    fn default() -> Self {
        BlockParser::<DefaultDecompressor>::new()
    }
}

impl<D: Decompressor> BlockParser<D> {
    /// Creates a new `BlockParser`.
    pub fn new() -> Self {
        Self {
            block_buffer: Vec::new(),
            decompressor: Default::default(),
        }
    }

    /// Parses `raw_block` into a header, primitive or unknown block.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an error occurs during PBF parsing, decompression or validation.
    #[allow(deprecated)]
    pub fn parse_block(&mut self, raw_block: RawBlock) -> Result<Block, Error> {
        let blob = match pbf::Blob::decode(&*raw_block.data) {
            Ok(blob) => blob,
            Err(error) => return Err(Error::PbfParseError(error)),
        };

        if let Some(uncompressed_size) = blob.raw_size {
            let uncompressed_size: usize = uncompressed_size.try_into().map_err(|_err| Error::InvalidBlobData)?;
            self.block_buffer.resize_with(uncompressed_size, Default::default);
        }

        if let Some(blob_data) = blob.data {
            match blob_data {
                pbf::blob::Data::Raw(raw_data) => self.block_buffer.extend_from_slice(&raw_data),
                pbf::blob::Data::ZlibData(zlib_data) => {
                    if let Err(error) = D::decompress(CompressionMethod::Zlib, &zlib_data, &mut self.block_buffer) {
                        return Err(Error::DecompressionError(error));
                    }
                }
                pbf::blob::Data::Lz4Data(lz4_data) => {
                    if let Err(error) = D::decompress(CompressionMethod::Lz4, &lz4_data, &mut self.block_buffer) {
                        return Err(Error::DecompressionError(error));
                    }
                }
                pbf::blob::Data::LzmaData(lzma_data) => {
                    if let Err(error) = D::decompress(CompressionMethod::Lzma, &lzma_data, &mut self.block_buffer) {
                        return Err(Error::DecompressionError(error));
                    }
                }
                pbf::blob::Data::ZstdData(zstd_data) => {
                    if let Err(error) = D::decompress(CompressionMethod::Zstd, &zstd_data, &mut self.block_buffer) {
                        return Err(Error::DecompressionError(error));
                    }
                }
                pbf::blob::Data::ObsoleteBzip2Data(_) => return Err(Error::InvalidBlobData),
            }
        } else {
            return Err(Error::InvalidBlobData);
        }

        match raw_block.r#type {
            BlockType::Header => match pbf::HeaderBlock::decode(&*self.block_buffer) {
                Ok(header_block) => Ok(Block::Header(header_block)),
                Err(error) => Err(Error::PbfParseError(error)),
            },
            BlockType::Primitive => match pbf::PrimitiveBlock::decode(&*self.block_buffer) {
                Ok(primitive_block) => Ok(Block::Primitive(primitive_block)),
                Err(error) => Err(Error::PbfParseError(error)),
            },
            BlockType::Unknown => Ok(Block::Unknown(&self.block_buffer)),
        }
    }
}

/// Generalized implementation for reading normal or densely encoded tags from string tables.
///
/// Use [`new_tag_reader`] or [`dense::new_dense_tag_reader`] to construct it.
pub struct TagReader<'a, I>
where
    I: Iterator<Item = (Result<usize, Error>, Result<usize, Error>)>,
{
    string_table: &'a pbf::StringTable,
    iter: I,
}

impl<'a, I> Iterator for TagReader<'a, I>
where
    I: Iterator<Item = (Result<usize, Error>, Result<usize, Error>)>,
{
    /// Tag as a (key, value) pair, containing either a string or an error if decoding has failed
    type Item = (Result<&'a str, Error>, Result<&'a str, Error>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((key, value)) => {
                let decode_string = |index: usize| -> Result<&str, Error> {
                    if let Some(bytes) = self.string_table.s.get(index) {
                        if let Ok(utf8_string) = str::from_utf8(bytes) {
                            Ok(utf8_string)
                        } else {
                            Err(Error::LogicError(format!("string at index {index} is not valid UTF-8")))
                        }
                    } else {
                        Err(Error::LogicError(format!(
                            "string table index {index} is out of bounds ({})",
                            self.string_table.s.len()
                        )))
                    }
                };

                let key = match key {
                    Ok(key_idx) => decode_string(key_idx),
                    Err(error) => Err(error),
                };

                let value = match value {
                    Ok(value_idx) => decode_string(value_idx),
                    Err(error) => Err(error),
                };

                Some((key, value))
            }
            None => None,
        }
    }
}

/// Constructs a new `TagReader` from key and value index slices, and a corresponding string table.
///
/// # Examples
///
/// ```no_run
/// use rosm_pbf_reader::{pbf, new_tag_reader};
///
/// fn process_primitive_block(block: pbf::PrimitiveBlock) {
///     for group in &block.primitivegroup {
///         for way in &group.ways {
///             let tags = new_tag_reader(&block.stringtable, &way.keys, &way.vals);
///             for (key, value) in tags {
///                 println!("{}: {}", key.unwrap(), value.unwrap());
///             }
///         }
///     }
/// }
pub fn new_tag_reader<'a>(
    string_table: &'a pbf::StringTable,
    key_indices: &'a [u32],
    value_indices: &'a [u32],
) -> TagReader<'a, impl Iterator<Item = (Result<usize, Error>, Result<usize, Error>)> + 'a> {
    TagReader {
        string_table,
        iter: key_indices
            .iter()
            .map(|i| Ok(*i as usize))
            .zip(value_indices.iter().map(|i| Ok(*i as usize))),
    }
}

#[cfg(test)]
mod tag_reader_tests {
    use super::*;

    #[test]
    fn valid_input() {
        let key_vals = ["", "key1", "val1", "key2", "val2"];
        let string_table = pbf::StringTable {
            s: key_vals.iter().map(|s| s.as_bytes().to_vec()).collect(),
        };

        let key_indices = [1, 3];
        let value_indices = [2, 4];
        let mut reader = new_tag_reader(&string_table, &key_indices, &value_indices);

        matches!(reader.next(), Some((Ok("key1"), Ok("val1"))));
        matches!(reader.next(), Some((Ok("key2"), Ok("val2"))));

        assert!(reader.next().is_none());
    }
}

/// Utility for reading delta-encoded values directly, like [`pbf::Way::refs`] and [`pbf::Relation::memids`].
pub struct DeltaValueReader<'a, T> {
    remaining: &'a [T],
    accumulated: T,
}

impl<'a, T> DeltaValueReader<'a, T>
where
    T: std::default::Default,
{
    /// Constructs a new `DeltaValueReader` from a slice of values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rosm_pbf_reader::{pbf, DeltaValueReader};
    ///
    /// fn process_primitive_block(block: pbf::PrimitiveBlock) {
    ///     for group in &block.primitivegroup {
    ///         for way in &group.ways {
    ///             let refs = DeltaValueReader::new(&way.refs);
    ///             for node_id in refs {
    ///                 println!("{}", node_id);
    ///             }
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

impl<T> Iterator for DeltaValueReader<'_, T>
where
    T: std::ops::AddAssign + std::clone::Clone,
{
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

#[cfg(test)]
mod delta_value_reader_tests {
    use super::*;

    #[test]
    fn empty_input() {
        let mut reader = DeltaValueReader::new(&[] as &[i64]);
        assert_eq!(reader.next(), None);
    }

    #[test]
    fn valid_input() {
        let values = [10, -1, 4, -2];
        let mut reader = DeltaValueReader::new(&values);
        assert_eq!(reader.next(), Some(10));
        assert_eq!(reader.next(), Some(9));
        assert_eq!(reader.next(), Some(13));
        assert_eq!(reader.next(), Some(11));
    }
}
