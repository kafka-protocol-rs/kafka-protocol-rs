//! Provides utilities for working with records (Kafka messages).
//!
//! [`FetchResponse`](crate::messages::fetch_response::FetchResponse) and associated APIs for interacting with reading and writing
//! contain records in a raw format, allowing the user to implement their own logic for interacting
//! with those values.
//!
//! # Example
//!
//! Decoding a set of records from a [`FetchResponse`](crate::messages::fetch_response::FetchResponse):
//! ```rust
//! use kafka_protocol::messages::FetchResponse;
//! use kafka_protocol::protocol::Decodable;
//! use kafka_protocol::records::RecordBatchDecoder;
//! use bytes::Bytes;
//! use kafka_protocol::records::Compression;
//!
//! # const HEADER: [u8; 45] = [ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,];
//! # const RECORD: [u8; 79] = [ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x43, 0x0, 0x0, 0x0, 0x0, 0x2, 0x73, 0x6d, 0x29, 0x7b, 0x0, 0b00000000, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x22, 0x1, 0xd0, 0xf, 0x2, 0xa, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0xa, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x0,];
//! # let mut res = vec![];
//! # res.extend_from_slice(&HEADER[..]);
//! # res.extend_from_slice(&[0x00, 0x00, 0x00, 0x4f]);
//! # res.extend_from_slice(&RECORD[..]);
//! # let mut buf = Bytes::from(res);
//!
//! let res = FetchResponse::decode(&mut buf, 4).unwrap();
//!
//! for topic in res.responses {
//!     for partition in topic.partitions {
//!          let mut records = partition.records.unwrap();
//!          let records = RecordBatchDecoder::decode_with_custom_compression(&mut records, Some(decompress_record_batch_data)).unwrap();
//!     }
//! }
//!
//! fn decompress_record_batch_data(compressed_buffer: &mut bytes::Bytes, compression: Compression) -> anyhow::Result<Bytes> {
//!         match compression {
//!             Compression::None => Ok(compressed_buffer.to_vec().into()),
//!             _ => { panic!("Compression not implemented") }
//!         }
//!  }
//! ```
use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use crc::{Crc, CRC_32_ISO_HDLC};
use crc32c::crc32c;
use indexmap::IndexMap;

use crate::protocol::{
    buf::{gap, ByteBuf, ByteBufMut},
    types, Decoder, Encoder, StrBytes,
};

use super::compression::{self as cmpr, Compressor, Decompressor};
use std::cmp::Ordering;
use std::convert::TryFrom;
/// IEEE (checksum) cyclic redundancy check.
pub const IEEE: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// The different types of compression supported by Kafka.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    None = 0,
    /// gzip compression library.
    Gzip = 1,
    /// Google's Snappy compression library.
    Snappy = 2,
    /// The LZ4 compression library.
    Lz4 = 3,
    /// Facebook's ZStandard compression library.
    Zstd = 4,
}

/// Indicates the meaning of the timestamp field on a record.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimestampType {
    /// The timestamp represents when the record was created by the client.
    Creation = 0,
    /// The timestamp represents when the record was appended to the log.
    LogAppend = 1,
}

/// Options for encoding and compressing a batch of records. Note, not all compression algorithms
/// are currently implemented by this library.
pub struct RecordEncodeOptions {
    /// Record version, 0, 1, or 2.
    pub version: i8,

    /// The compression algorithm to use.
    pub compression: Compression,
}

/// Value to indicate missing producer id.
pub const NO_PRODUCER_ID: i64 = -1;
/// Value to indicate missing producer epoch.
pub const NO_PRODUCER_EPOCH: i16 = -1;
/// Value to indicated missing leader epoch.
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;
/// Value to indicate missing sequence id.
pub const NO_SEQUENCE: i32 = -1;
/// Value to indicate missing timestamp.
pub const NO_TIMESTAMP: i64 = -1;

#[derive(Debug, Clone)]
/// Batch encoder for Kafka records.
pub struct RecordBatchEncoder;

#[derive(Debug, Clone)]
/// Batch decoder for Kafka records.
pub struct RecordBatchDecoder;

struct BatchDecodeInfo {
    record_count: usize,
    timestamp_type: TimestampType,
    min_offset: i64,
    min_timestamp: i64,
    base_sequence: i32,
    transactional: bool,
    control: bool,
    delete_horizon: bool,
    partition_leader_epoch: i32,
    producer_id: i64,
    producer_epoch: i16,
}

/// Decoded records plus information about compression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSet {
    /// Compression used for this set of records
    pub compression: Compression,
    /// Version used to encode the set of records
    pub version: i8,
    /// Records decoded in this set
    pub records: Vec<Record>,
}

/// A Kafka message containing key, payload value, and all associated metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    // Batch properties
    /// Whether this record is transactional.
    pub transactional: bool,
    /// Whether this record is a control message, which should not be exposed to the client.
    pub control: bool,
    /// Whether this record has the delete horizon flag set.
    pub delete_horizon: bool,
    /// Epoch of the leader for this record 's partition.
    pub partition_leader_epoch: i32,
    /// The identifier of the producer.
    pub producer_id: i64,
    /// Producer metadata used to implement transactional writes.
    pub producer_epoch: i16,

    // Record properties
    /// Indicates whether timestamp represents record creation or appending to the log.
    pub timestamp_type: TimestampType,
    /// Message offset within a partition.
    pub offset: i64,
    /// Sequence identifier used for idempotent delivery.
    pub sequence: i32,
    /// Timestamp the record. See also `timestamp_type`.
    pub timestamp: i64,
    /// The key of the record.
    pub key: Option<Bytes>,
    /// The payload of the record.
    pub value: Option<Bytes>,
    /// Headers associated with the record's payload.
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
}

const MAGIC_BYTE_OFFSET: usize = 16;

impl RecordBatchEncoder {
    /// Encode records into given buffer, using provided encoding options that select the encoding
    /// strategy based on version.
    pub fn encode<'a, B, I>(buf: &mut B, records: I, options: &RecordEncodeOptions) -> Result<()>
    where
        B: ByteBufMut,
        I: IntoIterator<Item = &'a Record>,
        I::IntoIter: Clone,
    {
        Self::encode_with_custom_compression(
            buf,
            records,
            options,
            None::<fn(&mut BytesMut, &mut B, Compression) -> Result<()>>,
        )
    }

    /// Encode records into given buffer, using provided encoding options that select the encoding
    /// strategy based on version.
    /// # Arguments
    /// * `compressor` - A function that compresses the given batch of records.
    ///
    /// If `None`, the right compression algorithm will automatically be selected and applied.
    pub fn encode_with_custom_compression<'a, B, I, CF>(
        buf: &mut B,
        records: I,
        options: &RecordEncodeOptions,
        compressor: Option<CF>,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: IntoIterator<Item = &'a Record>,
        I::IntoIter: Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        let records = records.into_iter();
        match options.version {
            0..=1 => bail!("message sets v{} are unsupported", options.version),
            2 => Self::encode_new(buf, records, options, compressor),
            _ => bail!("Unknown record batch version"),
        }
    }

    fn encode_new_records<'a, B, I>(
        buf: &mut B,
        records: I,
        min_offset: i64,
        min_timestamp: i64,
        options: &RecordEncodeOptions,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record>,
    {
        for record in records {
            record.encode_new(buf, min_offset, min_timestamp, options)?;
        }
        Ok(())
    }

    fn encode_new_batch<'a, B, I, CF>(
        buf: &mut B,
        records: &mut I,
        options: &RecordEncodeOptions,
        compressor: Option<&CF>,
    ) -> Result<bool>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record> + Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        let mut record_peeker = records.clone();

        // Get first record
        let first_record = match record_peeker.next() {
            Some(record) => record,
            None => return Ok(false),
        };

        // Determine how many additional records can be included in the batch
        let num_records = record_peeker
            .take_while(|record| {
                record.transactional == first_record.transactional
                    && record.control == first_record.control
                    && record.delete_horizon == first_record.delete_horizon
                    && record.partition_leader_epoch == first_record.partition_leader_epoch
                    && record.producer_id == first_record.producer_id
                    && record.producer_epoch == first_record.producer_epoch
                    && (record.offset as i32).wrapping_sub(record.sequence)
                        == (first_record.offset as i32).wrapping_sub(first_record.sequence)
            })
            .count()
            + 1;

        // Aggregate various record properties
        let min_offset = records
            .clone()
            .take(num_records)
            .map(|r| r.offset)
            .min()
            .expect("Batch contains at least one element");
        let max_offset = records
            .clone()
            .take(num_records)
            .map(|r| r.offset)
            .max()
            .expect("Batch contains at least one element");
        let min_timestamp = records
            .clone()
            .take(num_records)
            .map(|r| r.timestamp)
            .min()
            .expect("Batch contains at least one element");
        let max_timestamp = records
            .clone()
            .take(num_records)
            .map(|r| r.timestamp)
            .max()
            .expect("Batch contains at least one element");
        let base_sequence = first_record
            .sequence
            .wrapping_sub((first_record.offset - min_offset) as i32);

        // Base offset
        types::Int64.encode(buf, min_offset)?;

        // Batch length
        let size_gap = buf.put_typed_gap(gap::I32);
        let batch_start = buf.offset();

        // Partition leader epoch
        types::Int32.encode(buf, first_record.partition_leader_epoch)?;

        // Magic byte
        types::Int8.encode(buf, options.version)?;

        // CRC
        let crc_gap = buf.put_typed_gap(gap::U32);
        let content_start = buf.offset();

        // Attributes
        let mut attributes = options.compression as i16;
        if first_record.transactional {
            attributes |= 1 << 4;
        }
        if first_record.control {
            attributes |= 1 << 5;
        }
        if first_record.delete_horizon {
            attributes |= 1 << 6;
        }
        types::Int16.encode(buf, attributes)?;

        // Last offset delta
        types::Int32.encode(buf, (max_offset - min_offset) as i32)?;

        // First timestamp
        types::Int64.encode(buf, min_timestamp)?;

        // Last timestamp
        types::Int64.encode(buf, max_timestamp)?;

        // Producer ID
        types::Int64.encode(buf, first_record.producer_id)?;

        // Producer epoch
        types::Int16.encode(buf, first_record.producer_epoch)?;

        // Base sequence
        types::Int32.encode(buf, base_sequence)?;

        // Record count
        if num_records > i32::MAX as usize {
            bail!(
                "Too many records to encode in one batch ({num_records} records)"
            );
        }
        types::Int32.encode(buf, num_records as i32)?;

        // Records
        let records = records.take(num_records);

        if let Some(compressor) = compressor {
            let mut record_buf = BytesMut::new();
            Self::encode_new_records(&mut record_buf, records, min_offset, min_timestamp, options)?;
            compressor(&mut record_buf, buf, options.compression)?;
        } else {
            match options.compression {
                Compression::None => cmpr::None::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, min_offset, min_timestamp, options)
                })?,
                #[cfg(feature = "snappy")]
                Compression::Snappy => cmpr::Snappy::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, min_offset, min_timestamp, options)
                })?,
                #[cfg(feature = "gzip")]
                Compression::Gzip => cmpr::Gzip::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, min_offset, min_timestamp, options)
                })?,
                #[cfg(feature = "lz4")]
                Compression::Lz4 => cmpr::Lz4::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, min_offset, min_timestamp, options)
                })?,
                #[cfg(feature = "zstd")]
                Compression::Zstd => cmpr::Zstd::compress(buf, |buf| {
                    Self::encode_new_records(buf, records, min_offset, min_timestamp, options)
                })?,
                #[allow(unreachable_patterns)]
                c => {
                    return Err(anyhow!(
                        "Support for {c:?} is not enabled as a cargo feature"
                    ))
                }
            }
        }
        let batch_end = buf.offset();

        // Fill size gap
        let batch_size = batch_end - batch_start;
        if batch_size > i32::MAX as usize {
            bail!(
                "Record batch was too large to encode ({batch_size} bytes)"
            );
        }

        buf.fill_typed_gap(size_gap, batch_size as i32);

        // Fill CRC gap
        let crc = crc32c(buf.range(content_start..batch_end));
        buf.fill_typed_gap(crc_gap, crc);

        Ok(true)
    }

    fn encode_new<'a, B, I, CF>(
        buf: &mut B,
        mut records: I,
        options: &RecordEncodeOptions,
        compressor: Option<CF>,
    ) -> Result<()>
    where
        B: ByteBufMut,
        I: Iterator<Item = &'a Record> + Clone,
        CF: Fn(&mut BytesMut, &mut B, Compression) -> Result<()>,
    {
        while Self::encode_new_batch(buf, &mut records, options, compressor.as_ref())? {}
        Ok(())
    }
}

impl RecordBatchDecoder {
    /// Decode one RecordSet from the provided buffer.
    /// # Arguments
    /// * `decompressor` - A function that decompresses the given batch of records.
    ///
    /// If `None`, the right decompression algorithm will automatically be selected and applied.
    pub fn decode_with_custom_compression<B: ByteBuf, F>(
        buf: &mut B,
        decompressor: Option<F>,
    ) -> Result<RecordSet>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        let mut records = Vec::new();
        let (version, compression) =
            Self::decode_into_vec(buf, &mut records, decompressor.as_ref())?;
        Ok(RecordSet {
            version,
            compression,
            records,
        })
    }

    /// Decode the entire buffer into a vec of RecordSets.
    pub fn decode_all<B: ByteBuf>(buf: &mut B) -> Result<Vec<RecordSet>> {
        let mut batches = Vec::new();
        while buf.has_remaining() {
            batches.push(Self::decode(buf)?);
        }
        Ok(batches)
    }

    /// Decode one RecordSet from the provided buffer.
    pub fn decode<B: ByteBuf>(buf: &mut B) -> Result<RecordSet> {
        Self::decode_with_custom_compression(
            buf,
            None::<fn(&mut bytes::Bytes, Compression) -> Result<B>>.as_ref(),
        )
    }

    fn decode_into_vec<B: ByteBuf, F>(
        buf: &mut B,
        records: &mut Vec<Record>,
        decompress_func: Option<&F>,
    ) -> Result<(i8, Compression)>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        let version = buf.try_peek_bytes(MAGIC_BYTE_OFFSET..(MAGIC_BYTE_OFFSET + 1))?[0] as i8;
        let compression = match version {
            0..=1 => bail!("message sets v{version} are unsupported"),
            2 => Self::decode_new_batch(buf, version, records, decompress_func),
            _ => {
                bail!("Unknown record batch version ({version})");
            }
        }?;
        Ok((version, compression))
    }
    fn decode_new_records<B: ByteBuf>(
        buf: &mut B,
        batch_decode_info: &BatchDecodeInfo,
        version: i8,
        records: &mut Vec<Record>,
    ) -> Result<()> {
        records.reserve(batch_decode_info.record_count);
        for _ in 0..batch_decode_info.record_count {
            records.push(Record::decode_new(buf, batch_decode_info, version)?);
        }
        Ok(())
    }
    fn decode_new_batch<B: ByteBuf, F>(
        buf: &mut B,
        version: i8,
        records: &mut Vec<Record>,
        decompress_func: Option<&F>,
    ) -> Result<Compression>
    where
        F: Fn(&mut bytes::Bytes, Compression) -> Result<B>,
    {
        // Base offset
        let min_offset = types::Int64.decode(buf)?;

        // Batch length
        let batch_length: i32 = types::Int32.decode(buf)?;
        if batch_length < 0 {
            bail!("Unexpected negative batch size: {batch_length}");
        }

        // Convert buf to bytes
        let buf = &mut buf.try_get_bytes(batch_length as usize)?;

        // Partition leader epoch
        let partition_leader_epoch = types::Int32.decode(buf)?;

        // Magic byte
        let magic: i8 = types::Int8.decode(buf)?;
        if magic != version {
            bail!("Version mismatch ({magic} != {version})");
        }

        // CRC
        let supplied_crc: u32 = types::UInt32.decode(buf)?;
        let actual_crc = crc32c(buf);

        if supplied_crc != actual_crc {
            bail!(
                "Cyclic redundancy check failed ({supplied_crc} != {actual_crc})"
            );
        }

        // Attributes
        let attributes: i16 = types::Int16.decode(buf)?;
        let transactional = (attributes & (1 << 4)) != 0;
        let control = (attributes & (1 << 5)) != 0;
        let delete_horizon = (attributes & (1 << 6)) != 0;
        let compression = match attributes & 0x7 {
            0 => Compression::None,
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            other => {
                bail!("Unknown compression algorithm used: {other}");
            }
        };
        let timestamp_type = if (attributes & (1 << 3)) != 0 {
            TimestampType::LogAppend
        } else {
            TimestampType::Creation
        };

        // Last offset delta
        let _max_offset_delta: i32 = types::Int32.decode(buf)?;

        // First timestamp
        let min_timestamp = types::Int64.decode(buf)?;

        // Last timestamp
        let _max_timestamp: i64 = types::Int64.decode(buf)?;

        // Producer ID
        let producer_id = types::Int64.decode(buf)?;

        // Producer epoch
        let producer_epoch = types::Int16.decode(buf)?;

        // Base sequence
        let base_sequence = types::Int32.decode(buf)?;

        // Record count
        let record_count: i32 = types::Int32.decode(buf)?;
        if record_count < 0 {
            bail!("Unexpected negative record count ({record_count})");
        }
        let record_count = record_count as usize;

        let batch_decode_info = BatchDecodeInfo {
            record_count,
            timestamp_type,
            min_offset,
            min_timestamp,
            base_sequence,
            transactional,
            control,
            delete_horizon,
            partition_leader_epoch,
            producer_id,
            producer_epoch,
        };

        if let Some(decompress_func) = decompress_func {
            let mut decompressed_buf = decompress_func(buf, compression)?;

            Self::decode_new_records(&mut decompressed_buf, &batch_decode_info, version, records)?;
        } else {
            match compression {
                Compression::None => cmpr::None::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "snappy")]
                Compression::Snappy => cmpr::Snappy::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "gzip")]
                Compression::Gzip => cmpr::Gzip::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "zstd")]
                Compression::Zstd => cmpr::Zstd::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[cfg(feature = "lz4")]
                Compression::Lz4 => cmpr::Lz4::decompress(buf, |buf| {
                    Self::decode_new_records(buf, &batch_decode_info, version, records)
                })?,
                #[allow(unreachable_patterns)]
                c => {
                    return Err(anyhow!(
                        "Support for {c:?} is not enabled as a cargo feature"
                    ))
                }
            };
        }

        Ok(compression)
    }
}

impl Record {
    fn encode_new<B: ByteBufMut>(
        &self,
        buf: &mut B,
        min_offset: i64,
        min_timestamp: i64,
        options: &RecordEncodeOptions,
    ) -> Result<()> {
        // Size
        let size = self.compute_size_new(min_offset, min_timestamp, options)?;
        if size > i32::MAX as usize {
            bail!("Record was too large to encode ({size} bytes)");
        }
        types::VarInt.encode(buf, size as i32)?;

        // Attributes
        types::Int8.encode(buf, 0)?;

        // Timestamp delta
        let timestamp_delta = self.timestamp - min_timestamp;
        if timestamp_delta > i32::MAX as i64 || timestamp_delta < i32::MIN as i64 {
            bail!(
                "Timestamps within batch are too far apart ({}, {})",
                min_timestamp,
                self.timestamp
            );
        }
        types::VarInt.encode(buf, timestamp_delta as i32)?;

        // Offset delta
        let offset_delta = self.offset - min_offset;
        if offset_delta > i32::MAX as i64 || offset_delta < i32::MIN as i64 {
            bail!(
                "Timestamps within batch are too far apart ({}, {})",
                min_offset,
                self.offset
            );
        }
        types::VarInt.encode(buf, offset_delta as i32)?;

        // Key
        if let Some(k) = self.key.as_ref() {
            if k.len() > i32::MAX as usize {
                bail!("Record key was too large to encode ({} bytes)", k.len());
            }
            types::VarInt.encode(buf, k.len() as i32)?;
            buf.put_slice(k);
        } else {
            types::VarInt.encode(buf, -1)?;
        }

        // Value
        if let Some(v) = self.value.as_ref() {
            if v.len() > i32::MAX as usize {
                bail!("Record value was too large to encode ({} bytes)", v.len());
            }
            types::VarInt.encode(buf, v.len() as i32)?;
            buf.put_slice(v);
        } else {
            types::VarInt.encode(buf, -1)?;
        }

        // Headers
        if self.headers.len() > i32::MAX as usize {
            bail!("Too many record headers encode ({})", self.headers.len());
        }
        types::VarInt.encode(buf, self.headers.len() as i32)?;
        for (k, v) in &self.headers {
            // Key len
            if k.len() > i32::MAX as usize {
                bail!(
                    "Record header key was too large to encode ({} bytes)",
                    k.len()
                );
            }
            types::VarInt.encode(buf, k.len() as i32)?;

            // Key
            buf.put_slice(k.as_ref());

            // Value
            if let Some(v) = v.as_ref() {
                if v.len() > i32::MAX as usize {
                    bail!(
                        "Record header value was too large to encode ({} bytes)",
                        v.len()
                    );
                }
                types::VarInt.encode(buf, v.len() as i32)?;
                buf.put_slice(v);
            } else {
                types::VarInt.encode(buf, -1)?;
            }
        }

        Ok(())
    }
    fn compute_size_new(
        &self,
        min_offset: i64,
        min_timestamp: i64,
        _options: &RecordEncodeOptions,
    ) -> Result<usize> {
        let mut total_size = 0;

        // Attributes
        total_size += types::Int8.compute_size(0)?;

        // Timestamp delta
        let timestamp_delta = self.timestamp - min_timestamp;
        if timestamp_delta > i32::MAX as i64 || timestamp_delta < i32::MIN as i64 {
            bail!(
                "Timestamps within batch are too far apart ({}, {})",
                min_timestamp,
                self.timestamp
            );
        }
        total_size += types::VarInt.compute_size(timestamp_delta as i32)?;

        // Offset delta
        let offset_delta = self.offset - min_offset;
        if offset_delta > i32::MAX as i64 || offset_delta < i32::MIN as i64 {
            bail!(
                "Timestamps within batch are too far apart ({}, {})",
                min_offset,
                self.offset
            );
        }
        total_size += types::VarInt.compute_size(offset_delta as i32)?;

        // Key
        if let Some(k) = self.key.as_ref() {
            if k.len() > i32::MAX as usize {
                bail!("Record key was too large to encode ({} bytes)", k.len());
            }
            total_size += types::VarInt.compute_size(k.len() as i32)?;
            total_size += k.len();
        } else {
            total_size += types::VarInt.compute_size(-1)?;
        }

        // Value len
        if let Some(v) = self.value.as_ref() {
            if v.len() > i32::MAX as usize {
                bail!("Record value was too large to encode ({} bytes)", v.len());
            }
            total_size += types::VarInt.compute_size(v.len() as i32)?;
            total_size += v.len();
        } else {
            total_size += types::VarInt.compute_size(-1)?;
        }

        // Headers
        if self.headers.len() > i32::MAX as usize {
            bail!("Too many record headers encode ({})", self.headers.len());
        }
        total_size += types::VarInt.compute_size(self.headers.len() as i32)?;
        for (k, v) in &self.headers {
            // Key len
            if k.len() > i32::MAX as usize {
                bail!(
                    "Record header key was too large to encode ({} bytes)",
                    k.len()
                );
            }
            total_size += types::VarInt.compute_size(k.len() as i32)?;

            // Key
            total_size += k.len();

            // Value
            if let Some(v) = v.as_ref() {
                if v.len() > i32::MAX as usize {
                    bail!(
                        "Record header value was too large to encode ({} bytes)",
                        v.len()
                    );
                }
                total_size += types::VarInt.compute_size(v.len() as i32)?;
                total_size += v.len();
            } else {
                total_size += types::VarInt.compute_size(-1)?;
            }
        }

        Ok(total_size)
    }
    fn decode_new<B: ByteBuf>(
        buf: &mut B,
        batch_decode_info: &BatchDecodeInfo,
        _version: i8,
    ) -> Result<Self> {
        // Size
        let size: i32 = types::VarInt.decode(buf)?;
        if size < 0 {
            bail!("Unexpected negative record size: {size}");
        }

        // Ensure we don't over-read
        let buf = &mut buf.try_get_bytes(size as usize)?;

        // Attributes
        let _attributes: i8 = types::Int8.decode(buf)?;

        // Timestamp delta
        let timestamp_delta: i32 = types::VarInt.decode(buf)?;
        let timestamp = batch_decode_info.min_timestamp + timestamp_delta as i64;

        // Offset delta
        let offset_delta: i32 = types::VarInt.decode(buf)?;
        let offset = batch_decode_info.min_offset + offset_delta as i64;
        let sequence = batch_decode_info.base_sequence.wrapping_add(offset_delta);

        // Key
        let key_len: i32 = types::VarInt.decode(buf)?;
        let key = match key_len.cmp(&-1) {
            Ordering::Less => {
                bail!("Unexpected negative record key length ({key_len} bytes)");
            }
            Ordering::Equal => None,
            Ordering::Greater => Some(buf.try_get_bytes(key_len as usize)?),
        };

        // Value
        let value_len: i32 = types::VarInt.decode(buf)?;
        let value = match value_len.cmp(&-1) {
            Ordering::Less => {
                bail!(
                    "Unexpected negative record value length ({value_len} bytes)"
                );
            }
            Ordering::Equal => None,
            Ordering::Greater => Some(buf.try_get_bytes(value_len as usize)?),
        };

        // Headers
        let num_headers: i32 = types::VarInt.decode(buf)?;
        if num_headers < 0 {
            bail!("Unexpected negative record header count: {num_headers}");
        }
        let num_headers = num_headers as usize;

        let mut headers = IndexMap::with_capacity(num_headers);
        for _ in 0..num_headers {
            // Key len
            let key_len: i32 = types::VarInt.decode(buf)?;
            if key_len < 0 {
                bail!(
                    "Unexpected negative record header key length ({key_len} bytes)"
                );
            }

            // Key
            let key = StrBytes::try_from(buf.try_get_bytes(key_len as usize)?)?;

            // Key len
            let value_len: i32 = types::VarInt.decode(buf)?;

            // Value
            let value = match value_len.cmp(&-1) {
                Ordering::Less => {
                    bail!(
                        "Unexpected negative record header value length ({value_len} bytes)"
                    );
                }
                Ordering::Equal => None,
                Ordering::Greater => Some(buf.try_get_bytes(value_len as usize)?),
            };

            headers.insert(key, value);
        }

        Ok(Self {
            transactional: batch_decode_info.transactional,
            control: batch_decode_info.control,
            delete_horizon: batch_decode_info.delete_horizon,
            timestamp_type: batch_decode_info.timestamp_type,
            partition_leader_epoch: batch_decode_info.partition_leader_epoch,
            producer_id: batch_decode_info.producer_id,
            producer_epoch: batch_decode_info.producer_epoch,
            sequence,
            offset,
            timestamp,
            key,
            value,
            headers,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn lookup_header_via_u8_slice() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: Default::default(),
            value: Default::default(),
            headers: [
                ("some-key".into(), Some("some-value".into())),
                ("other-header".into(), None),
            ]
            .into(),
        };
        assert_eq!(
            Bytes::from("some-value"),
            record
                .headers
                // This relies on `impl Borrow<[u8]> for StrBytes`
                .get("some-key".as_bytes())
                .expect("key exists in headers")
                .as_ref()
                .expect("value is present")
        );
    }

    #[test]
    fn decode_record_header_no_value() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: Default::default(),
            value: Default::default(),
            headers: [("other-header".into(), None)].into(),
        };
        let mut buf = &mut bytes::BytesMut::new();
        record
            .encode_new(
                buf,
                0,
                0,
                &RecordEncodeOptions {
                    version: 2,
                    compression: super::Compression::None,
                },
            )
            .expect("encode works");

        Record::decode_new(
            &mut buf,
            &BatchDecodeInfo {
                record_count: 1,
                timestamp_type: TimestampType::Creation,
                min_offset: 0,
                min_timestamp: 0,
                base_sequence: 0,
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: 0,
                producer_epoch: 0,
            },
            2,
        )
        .expect("decode works");
    }
}
