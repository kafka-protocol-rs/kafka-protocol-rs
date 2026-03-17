//! ShareAcknowledgeRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ShareAcknowledgeRequest.json).
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
pub struct AcknowledgePartition {
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub partition_index: i32,

    /// Record batches to acknowledge.
    ///
    /// Supported API versions: 1
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AcknowledgePartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
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
impl Encodable for AcknowledgePartition {
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
impl Decodable for AcknowledgePartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
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
            acknowledgement_batches,
            unknown_tagged_fields,
        })
    }
}

impl Default for AcknowledgePartition {
    fn default() -> Self {
        Self {
            partition_index: 0,
            acknowledgement_batches: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AcknowledgePartition {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AcknowledgeTopic {
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub topic_id: Uuid,

    /// The partitions containing records to acknowledge.
    ///
    /// Supported API versions: 1
    pub partitions: Vec<AcknowledgePartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AcknowledgeTopic {
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
    /// The partitions containing records to acknowledge.
    ///
    /// Supported API versions: 1
    pub fn with_partitions(mut self, value: Vec<AcknowledgePartition>) -> Self {
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
impl Encodable for AcknowledgeTopic {
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
impl Decodable for AcknowledgeTopic {
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

impl Default for AcknowledgeTopic {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AcknowledgeTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

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
pub struct ShareAcknowledgeRequest {
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

    /// The topics containing records to acknowledge.
    ///
    /// Supported API versions: 1
    pub topics: Vec<AcknowledgeTopic>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareAcknowledgeRequest {
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
    /// Sets `topics` to the passed value.
    ///
    /// The topics containing records to acknowledge.
    ///
    /// Supported API versions: 1
    pub fn with_topics(mut self, value: Vec<AcknowledgeTopic>) -> Self {
        self.topics = value;
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
impl Encodable for ShareAcknowledgeRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.share_session_epoch)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
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
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
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
impl Decodable for ShareAcknowledgeRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let group_id = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let share_session_epoch = types::Int32.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for ShareAcknowledgeRequest {
    fn default() -> Self {
        Self {
            group_id: None,
            member_id: Some(Default::default()),
            share_session_epoch: 0,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareAcknowledgeRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ShareAcknowledgeRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
