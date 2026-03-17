//! VoteRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/VoteRequest.json).
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

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 0-2
    pub partition_index: i32,

    /// The epoch of the voter sending the request
    ///
    /// Supported API versions: 0-2
    pub replica_epoch: i32,

    /// The replica id of the voter sending the request
    ///
    /// Supported API versions: 0-2
    pub replica_id: super::BrokerId,

    /// The directory id of the voter sending the request
    ///
    /// Supported API versions: 1-2
    pub replica_directory_id: Uuid,

    /// The directory id of the voter receiving the request
    ///
    /// Supported API versions: 1-2
    pub voter_directory_id: Uuid,

    /// The epoch of the last record written to the metadata log.
    ///
    /// Supported API versions: 0-2
    pub last_offset_epoch: i32,

    /// The log end offset of the metadata log of the voter sending the request.
    ///
    /// Supported API versions: 0-2
    pub last_offset: i64,

    /// Whether the request is a PreVote request (not persisted) or not.
    ///
    /// Supported API versions: 2
    pub pre_vote: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionData {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0-2
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `replica_epoch` to the passed value.
    ///
    /// The epoch of the voter sending the request
    ///
    /// Supported API versions: 0-2
    pub fn with_replica_epoch(mut self, value: i32) -> Self {
        self.replica_epoch = value;
        self
    }
    /// Sets `replica_id` to the passed value.
    ///
    /// The replica id of the voter sending the request
    ///
    /// Supported API versions: 0-2
    pub fn with_replica_id(mut self, value: super::BrokerId) -> Self {
        self.replica_id = value;
        self
    }
    /// Sets `replica_directory_id` to the passed value.
    ///
    /// The directory id of the voter sending the request
    ///
    /// Supported API versions: 1-2
    pub fn with_replica_directory_id(mut self, value: Uuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    /// Sets `voter_directory_id` to the passed value.
    ///
    /// The directory id of the voter receiving the request
    ///
    /// Supported API versions: 1-2
    pub fn with_voter_directory_id(mut self, value: Uuid) -> Self {
        self.voter_directory_id = value;
        self
    }
    /// Sets `last_offset_epoch` to the passed value.
    ///
    /// The epoch of the last record written to the metadata log.
    ///
    /// Supported API versions: 0-2
    pub fn with_last_offset_epoch(mut self, value: i32) -> Self {
        self.last_offset_epoch = value;
        self
    }
    /// Sets `last_offset` to the passed value.
    ///
    /// The log end offset of the metadata log of the voter sending the request.
    ///
    /// Supported API versions: 0-2
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    /// Sets `pre_vote` to the passed value.
    ///
    /// Whether the request is a PreVote request (not persisted) or not.
    ///
    /// Supported API versions: 2
    pub fn with_pre_vote(mut self, value: bool) -> Self {
        self.pre_vote = value;
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
impl Encodable for PartitionData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int32.encode(buf, &self.replica_epoch)?;
        types::Int32.encode(buf, &self.replica_id)?;
        if version >= 1 {
            types::Uuid.encode(buf, &self.replica_directory_id)?;
        }
        if version >= 1 {
            types::Uuid.encode(buf, &self.voter_directory_id)?;
        }
        types::Int32.encode(buf, &self.last_offset_epoch)?;
        types::Int64.encode(buf, &self.last_offset)?;
        if version >= 2 {
            types::Boolean.encode(buf, &self.pre_vote)?;
        } else {
            if self.pre_vote {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
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
        total_size += types::Int32.compute_size(&self.replica_epoch)?;
        total_size += types::Int32.compute_size(&self.replica_id)?;
        if version >= 1 {
            total_size += types::Uuid.compute_size(&self.replica_directory_id)?;
        }
        if version >= 1 {
            total_size += types::Uuid.compute_size(&self.voter_directory_id)?;
        }
        total_size += types::Int32.compute_size(&self.last_offset_epoch)?;
        total_size += types::Int64.compute_size(&self.last_offset)?;
        if version >= 2 {
            total_size += types::Boolean.compute_size(&self.pre_vote)?;
        } else {
            if self.pre_vote {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
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
impl Decodable for PartitionData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let replica_epoch = types::Int32.decode(buf)?;
        let replica_id = types::Int32.decode(buf)?;
        let replica_directory_id = if version >= 1 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let voter_directory_id = if version >= 1 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let last_offset_epoch = types::Int32.decode(buf)?;
        let last_offset = types::Int64.decode(buf)?;
        let pre_vote = if version >= 2 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
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
            replica_epoch,
            replica_id,
            replica_directory_id,
            voter_directory_id,
            last_offset_epoch,
            last_offset,
            pre_vote,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            replica_epoch: 0,
            replica_id: (0).into(),
            replica_directory_id: Uuid::nil(),
            voter_directory_id: Uuid::nil(),
            last_offset_epoch: 0,
            last_offset: 0,
            pre_vote: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionData {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The topic name.
    ///
    /// Supported API versions: 0-2
    pub topic_name: super::TopicName,

    /// The partition data.
    ///
    /// Supported API versions: 0-2
    pub partitions: Vec<PartitionData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicData {
    /// Sets `topic_name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0-2
    pub fn with_topic_name(mut self, value: super::TopicName) -> Self {
        self.topic_name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partition data.
    ///
    /// Supported API versions: 0-2
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
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
impl Encodable for TopicData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.topic_name)?;
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
        total_size += types::CompactString.compute_size(&self.topic_name)?;
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
impl Decodable for TopicData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let topic_name = types::CompactString.decode(buf)?;
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
            topic_name,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic_name: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicData {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct VoteRequest {
    /// The cluster id.
    ///
    /// Supported API versions: 0-2
    pub cluster_id: Option<StrBytes>,

    /// The replica id of the voter receiving the request.
    ///
    /// Supported API versions: 1-2
    pub voter_id: super::BrokerId,

    /// The topic data.
    ///
    /// Supported API versions: 0-2
    pub topics: Vec<TopicData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl VoteRequest {
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster id.
    ///
    /// Supported API versions: 0-2
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `voter_id` to the passed value.
    ///
    /// The replica id of the voter receiving the request.
    ///
    /// Supported API versions: 1-2
    pub fn with_voter_id(mut self, value: super::BrokerId) -> Self {
        self.voter_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topic data.
    ///
    /// Supported API versions: 0-2
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
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
impl Encodable for VoteRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.cluster_id)?;
        if version >= 1 {
            types::Int32.encode(buf, &self.voter_id)?;
        }
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
        total_size += types::CompactString.compute_size(&self.cluster_id)?;
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.voter_id)?;
        }
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
impl Decodable for VoteRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let cluster_id = types::CompactString.decode(buf)?;
        let voter_id = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
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
            cluster_id,
            voter_id,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for VoteRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            voter_id: (-1).into(),
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for VoteRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for VoteRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
