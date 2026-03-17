//! ShareFetchRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ShareFetchRequest.json).
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

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AcknowledgementBatch {
    /// First offset of batch of records to acknowledge.
    ///
    /// Supported API versions: 1
    pub first_offset: i64,

    /// Last offset (inclusive) of batch of records to acknowledge.
    ///
    /// Supported API versions: 1
    pub last_offset: i64,

    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject.
    ///
    /// Supported API versions: 1
    pub acknowledge_types: Vec<i8>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AcknowledgementBatch {
    /// Sets `first_offset` to the passed value.
    ///
    /// First offset of batch of records to acknowledge.
    ///
    /// Supported API versions: 1
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
        self
    }
    /// Sets `last_offset` to the passed value.
    ///
    /// Last offset (inclusive) of batch of records to acknowledge.
    ///
    /// Supported API versions: 1
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    /// Sets `acknowledge_types` to the passed value.
    ///
    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject.
    ///
    /// Supported API versions: 1
    pub fn with_acknowledge_types(mut self, value: Vec<i8>) -> Self {
        self.acknowledge_types = value;
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

#[cfg(feature = "client")]
impl Encodable for AcknowledgementBatch {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.first_offset)?;
        types::Int64.encode(buf, &self.last_offset)?;
        types::CompactArray(types::Int8).encode(buf, &self.acknowledge_types)?;
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
        total_size += types::CompactArray(types::Int8).compute_size(&self.acknowledge_types)?;
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

#[cfg(feature = "broker")]
impl Decodable for AcknowledgementBatch {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let first_offset = types::Int64.decode(buf)?;
        let last_offset = types::Int64.decode(buf)?;
        let acknowledge_types = types::CompactArray(types::Int8).decode(buf)?;
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
            acknowledge_types,
            unknown_tagged_fields,
        })
    }
}

impl Default for AcknowledgementBatch {
    fn default() -> Self {
        Self {
            first_offset: 0,
            last_offset: 0,
            acknowledge_types: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AcknowledgementBatch {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub partition_index: i32,

    /// The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: none
    pub partition_max_bytes: i32,

    /// Record batches to acknowledge.
    ///
    /// Supported API versions: 1
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchPartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `partition_max_bytes` to the passed value.
    ///
    /// The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: none
    pub fn with_partition_max_bytes(mut self, value: i32) -> Self {
        self.partition_max_bytes = value;
        self
    }
    /// Sets `acknowledgement_batches` to the passed value.
    ///
    /// Record batches to acknowledge.
    ///
    /// Supported API versions: 1
    pub fn with_acknowledgement_batches(mut self, value: Vec<AcknowledgementBatch>) -> Self {
        self.acknowledgement_batches = value;
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

#[cfg(feature = "client")]
impl Encodable for FetchPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::CompactArray(types::Struct { version })
            .encode(buf, &self.acknowledgement_batches)?;
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
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.acknowledgement_batches)?;
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

#[cfg(feature = "broker")]
impl Decodable for FetchPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let partition_max_bytes = 0;
        let acknowledgement_batches = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            partition_index,
            partition_max_bytes,
            acknowledgement_batches,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchPartition {
    fn default() -> Self {
        Self {
            partition_index: 0,
            partition_max_bytes: 0,
            acknowledgement_batches: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchPartition {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub topic_id: Uuid,

    /// The partitions to fetch.
    ///
    /// Supported API versions: 1
    pub partitions: Vec<FetchPartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchTopic {
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions to fetch.
    ///
    /// Supported API versions: 1
    pub fn with_partitions(mut self, value: Vec<FetchPartition>) -> Self {
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

#[cfg(feature = "client")]
impl Encodable for FetchTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
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

#[cfg(feature = "broker")]
impl Decodable for FetchTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
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

impl Default for FetchTopic {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ForgottenTopic {
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub topic_id: Uuid,

    /// The partitions indexes to forget.
    ///
    /// Supported API versions: 1
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ForgottenTopic {
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions indexes to forget.
    ///
    /// Supported API versions: 1
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
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

#[cfg(feature = "client")]
impl Encodable for ForgottenTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.topic_id)?;
        types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
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
        total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
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

#[cfg(feature = "broker")]
impl Decodable for ForgottenTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = types::Uuid.decode(buf)?;
        let partitions = types::CompactArray(types::Int32).decode(buf)?;
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

impl Default for ForgottenTopic {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ForgottenTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ShareFetchRequest {
    /// The group identifier.
    ///
    /// Supported API versions: 1
    pub group_id: Option<super::GroupId>,

    /// The member ID.
    ///
    /// Supported API versions: 1
    pub member_id: Option<StrBytes>,

    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    ///
    /// Supported API versions: 1
    pub share_session_epoch: i32,

    /// The maximum time in milliseconds to wait for the response.
    ///
    /// Supported API versions: 1
    pub max_wait_ms: i32,

    /// The minimum bytes to accumulate in the response.
    ///
    /// Supported API versions: 1
    pub min_bytes: i32,

    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 1
    pub max_bytes: i32,

    /// The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
    ///
    /// Supported API versions: 1
    pub max_records: i32,

    /// The optimal number of records for batches of acquired records and acknowledgements.
    ///
    /// Supported API versions: 1
    pub batch_size: i32,

    /// The topics to fetch.
    ///
    /// Supported API versions: 1
    pub topics: Vec<FetchTopic>,

    /// The partitions to remove from this share session.
    ///
    /// Supported API versions: 1
    pub forgotten_topics_data: Vec<ForgottenTopic>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareFetchRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The group identifier.
    ///
    /// Supported API versions: 1
    pub fn with_group_id(mut self, value: Option<super::GroupId>) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID.
    ///
    /// Supported API versions: 1
    pub fn with_member_id(mut self, value: Option<StrBytes>) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `share_session_epoch` to the passed value.
    ///
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    ///
    /// Supported API versions: 1
    pub fn with_share_session_epoch(mut self, value: i32) -> Self {
        self.share_session_epoch = value;
        self
    }
    /// Sets `max_wait_ms` to the passed value.
    ///
    /// The maximum time in milliseconds to wait for the response.
    ///
    /// Supported API versions: 1
    pub fn with_max_wait_ms(mut self, value: i32) -> Self {
        self.max_wait_ms = value;
        self
    }
    /// Sets `min_bytes` to the passed value.
    ///
    /// The minimum bytes to accumulate in the response.
    ///
    /// Supported API versions: 1
    pub fn with_min_bytes(mut self, value: i32) -> Self {
        self.min_bytes = value;
        self
    }
    /// Sets `max_bytes` to the passed value.
    ///
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 1
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    /// Sets `max_records` to the passed value.
    ///
    /// The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
    ///
    /// Supported API versions: 1
    pub fn with_max_records(mut self, value: i32) -> Self {
        self.max_records = value;
        self
    }
    /// Sets `batch_size` to the passed value.
    ///
    /// The optimal number of records for batches of acquired records and acknowledgements.
    ///
    /// Supported API versions: 1
    pub fn with_batch_size(mut self, value: i32) -> Self {
        self.batch_size = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to fetch.
    ///
    /// Supported API versions: 1
    pub fn with_topics(mut self, value: Vec<FetchTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `forgotten_topics_data` to the passed value.
    ///
    /// The partitions to remove from this share session.
    ///
    /// Supported API versions: 1
    pub fn with_forgotten_topics_data(mut self, value: Vec<ForgottenTopic>) -> Self {
        self.forgotten_topics_data = value;
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

#[cfg(feature = "client")]
impl Encodable for ShareFetchRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.share_session_epoch)?;
        types::Int32.encode(buf, &self.max_wait_ms)?;
        types::Int32.encode(buf, &self.min_bytes)?;
        types::Int32.encode(buf, &self.max_bytes)?;
        types::Int32.encode(buf, &self.max_records)?;
        types::Int32.encode(buf, &self.batch_size)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.forgotten_topics_data)?;
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
        total_size += types::CompactString.compute_size(&self.group_id)?;
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.share_session_epoch)?;
        total_size += types::Int32.compute_size(&self.max_wait_ms)?;
        total_size += types::Int32.compute_size(&self.min_bytes)?;
        total_size += types::Int32.compute_size(&self.max_bytes)?;
        total_size += types::Int32.compute_size(&self.max_records)?;
        total_size += types::Int32.compute_size(&self.batch_size)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.forgotten_topics_data)?;
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

#[cfg(feature = "broker")]
impl Decodable for ShareFetchRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let group_id = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let share_session_epoch = types::Int32.decode(buf)?;
        let max_wait_ms = types::Int32.decode(buf)?;
        let min_bytes = types::Int32.decode(buf)?;
        let max_bytes = types::Int32.decode(buf)?;
        let max_records = types::Int32.decode(buf)?;
        let batch_size = types::Int32.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let forgotten_topics_data = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            topics,
            forgotten_topics_data,
            unknown_tagged_fields,
        })
    }
}

impl Default for ShareFetchRequest {
    fn default() -> Self {
        Self {
            group_id: None,
            member_id: Some(Default::default()),
            share_session_epoch: 0,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0x7fffffff,
            max_records: 0,
            batch_size: 0,
            topics: Default::default(),
            forgotten_topics_data: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareFetchRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ShareFetchRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
