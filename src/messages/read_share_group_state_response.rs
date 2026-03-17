//! ReadShareGroupStateResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ReadShareGroupStateResponse.json).
// WARNING: the items of this module are generated and should not be edited directly
#![allow(unused)]

use std::borrow::Borrow;
use std::collections::BTreeMap;

use crate::error::{bail, Result};
use bytes::Bytes;
use uuid::Uuid;

use crate::protocol::{
    buf::{ByteBuf, ByteBufMut},
    compute_unknown_tagged_fields_size, types, write_unknown_tagged_fields, Decodable, Decoder,
    Encodable, Encoder, HeaderVersion, Message, StrBytes, VersionRange,
};

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionResult {
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub partition: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 0
    pub error_message: Option<StrBytes>,

    /// The state epoch of the share-partition.
    ///
    /// Supported API versions: 0
    pub state_epoch: i32,

    /// The share-partition start offset, which can be -1 if it is not yet initialized.
    ///
    /// Supported API versions: 0
    pub start_offset: i64,

    /// The state batches for this share-partition.
    ///
    /// Supported API versions: 0
    pub state_batches: Vec<StateBatch>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionResult {
    /// Sets `partition` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `state_epoch` to the passed value.
    ///
    /// The state epoch of the share-partition.
    ///
    /// Supported API versions: 0
    pub fn with_state_epoch(mut self, value: i32) -> Self {
        self.state_epoch = value;
        self
    }
    /// Sets `start_offset` to the passed value.
    ///
    /// The share-partition start offset, which can be -1 if it is not yet initialized.
    ///
    /// Supported API versions: 0
    pub fn with_start_offset(mut self, value: i64) -> Self {
        self.start_offset = value;
        self
    }
    /// Sets `state_batches` to the passed value.
    ///
    /// The state batches for this share-partition.
    ///
    /// Supported API versions: 0
    pub fn with_state_batches(mut self, value: Vec<StateBatch>) -> Self {
        self.state_batches = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for PartitionResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::Int32.encode(buf, &self.state_epoch)?;
        types::Int64.encode(buf, &self.start_offset)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.state_batches)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::Int32.compute_size(&self.state_epoch)?;
        total_size += types::Int64.compute_size(&self.start_offset)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.state_batches)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for PartitionResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let partition = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let state_epoch = types::Int32.decode(buf)?;
        let start_offset = types::Int64.decode(buf)?;
        let state_batches = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            partition,
            error_code,
            error_message,
            state_epoch,
            start_offset,
            state_batches,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionResult {
    fn default() -> Self {
        Self {
            partition: 0,
            error_code: 0,
            error_message: None,
            state_epoch: 0,
            start_offset: 0,
            state_batches: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionResult {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ReadShareGroupStateResponse {
    /// The read results.
    ///
    /// Supported API versions: 0
    pub results: Vec<ReadStateResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ReadShareGroupStateResponse {
    /// Sets `results` to the passed value.
    ///
    /// The read results.
    ///
    /// Supported API versions: 0
    pub fn with_results(mut self, value: Vec<ReadStateResult>) -> Self {
        self.results = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for ReadShareGroupStateResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.results)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.results)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for ReadShareGroupStateResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let results = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            results,
            unknown_tagged_fields,
        })
    }
}

impl Default for ReadShareGroupStateResponse {
    fn default() -> Self {
        Self {
            results: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ReadShareGroupStateResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ReadStateResult {
    /// The topic identifier.
    ///
    /// Supported API versions: 0
    pub topic_id: Uuid,

    /// The results for the partitions.
    ///
    /// Supported API versions: 0
    pub partitions: Vec<PartitionResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ReadStateResult {
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic identifier.
    ///
    /// Supported API versions: 0
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The results for the partitions.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<PartitionResult>) -> Self {
        self.partitions = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for ReadStateResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.topic_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Uuid.compute_size(&self.topic_id)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for ReadStateResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = types::Uuid.decode(buf)?;
        let partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            topic_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for ReadStateResult {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ReadStateResult {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct StateBatch {
    /// The first offset of this state batch.
    ///
    /// Supported API versions: 0
    pub first_offset: i64,

    /// The last offset of this state batch.
    ///
    /// Supported API versions: 0
    pub last_offset: i64,

    /// The delivery state - 0:Available,2:Acked,4:Archived.
    ///
    /// Supported API versions: 0
    pub delivery_state: i8,

    /// The delivery count.
    ///
    /// Supported API versions: 0
    pub delivery_count: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl StateBatch {
    /// Sets `first_offset` to the passed value.
    ///
    /// The first offset of this state batch.
    ///
    /// Supported API versions: 0
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
        self
    }
    /// Sets `last_offset` to the passed value.
    ///
    /// The last offset of this state batch.
    ///
    /// Supported API versions: 0
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    /// Sets `delivery_state` to the passed value.
    ///
    /// The delivery state - 0:Available,2:Acked,4:Archived.
    ///
    /// Supported API versions: 0
    pub fn with_delivery_state(mut self, value: i8) -> Self {
        self.delivery_state = value;
        self
    }
    /// Sets `delivery_count` to the passed value.
    ///
    /// The delivery count.
    ///
    /// Supported API versions: 0
    pub fn with_delivery_count(mut self, value: i16) -> Self {
        self.delivery_count = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for StateBatch {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.first_offset)?;
        types::Int64.encode(buf, &self.last_offset)?;
        types::Int8.encode(buf, &self.delivery_state)?;
        types::Int16.encode(buf, &self.delivery_count)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int64.compute_size(&self.first_offset)?;
        total_size += types::Int64.compute_size(&self.last_offset)?;
        total_size += types::Int8.compute_size(&self.delivery_state)?;
        total_size += types::Int16.compute_size(&self.delivery_count)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for StateBatch {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let first_offset = types::Int64.decode(buf)?;
        let last_offset = types::Int64.decode(buf)?;
        let delivery_state = types::Int8.decode(buf)?;
        let delivery_count = types::Int16.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
            unknown_tagged_fields,
        })
    }
}

impl Default for StateBatch {
    fn default() -> Self {
        Self {
            first_offset: 0,
            last_offset: 0,
            delivery_state: 0,
            delivery_count: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for StateBatch {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ReadShareGroupStateResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
