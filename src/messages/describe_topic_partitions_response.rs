//! DescribeTopicPartitionsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json).
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
pub struct Cursor {
    /// The name for the first topic to process.
    ///
    /// Supported API versions: 0
    pub topic_name: super::TopicName,

    /// The partition index to start with.
    ///
    /// Supported API versions: 0
    pub partition_index: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Cursor {
    /// Sets `topic_name` to the passed value.
    ///
    /// The name for the first topic to process.
    ///
    /// Supported API versions: 0
    pub fn with_topic_name(mut self, value: super::TopicName) -> Self {
        self.topic_name = value;
        self
    }
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index to start with.
    ///
    /// Supported API versions: 0
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
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
impl Encodable for Cursor {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.topic_name)?;
        types::Int32.encode(buf, &self.partition_index)?;
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
        total_size += types::Int32.compute_size(&self.partition_index)?;
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
impl Decodable for Cursor {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let topic_name = types::CompactString.decode(buf)?;
        let partition_index = types::Int32.decode(buf)?;
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
            partition_index,
            unknown_tagged_fields,
        })
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            topic_name: Default::default(),
            partition_index: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Cursor {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub throttle_time_ms: i32,

    /// Each topic in the response.
    ///
    /// Supported API versions: 0
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,

    /// The next topic and partition index to fetch details for.
    ///
    /// Supported API versions: 0
    pub next_cursor: Option<Cursor>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeTopicPartitionsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Each topic in the response.
    ///
    /// Supported API versions: 0
    pub fn with_topics(mut self, value: Vec<DescribeTopicPartitionsResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `next_cursor` to the passed value.
    ///
    /// The next topic and partition index to fetch details for.
    ///
    /// Supported API versions: 0
    pub fn with_next_cursor(mut self, value: Option<Cursor>) -> Self {
        self.next_cursor = value;
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
impl Encodable for DescribeTopicPartitionsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        types::OptionStruct { version }.encode(buf, &self.next_cursor)?;
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        total_size += types::OptionStruct { version }.compute_size(&self.next_cursor)?;
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
impl Decodable for DescribeTopicPartitionsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let next_cursor = types::OptionStruct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeTopicPartitionsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            topics: Default::default(),
            next_cursor: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeTopicPartitionsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The partition index.
    ///
    /// Supported API versions: 0
    pub partition_index: i32,

    /// The ID of the leader broker.
    ///
    /// Supported API versions: 0
    pub leader_id: super::BrokerId,

    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 0
    pub leader_epoch: i32,

    /// The set of all nodes that host this partition.
    ///
    /// Supported API versions: 0
    pub replica_nodes: Vec<super::BrokerId>,

    /// The set of nodes that are in sync with the leader for this partition.
    ///
    /// Supported API versions: 0
    pub isr_nodes: Vec<super::BrokerId>,

    /// The new eligible leader replicas otherwise.
    ///
    /// Supported API versions: 0
    pub eligible_leader_replicas: Option<Vec<super::BrokerId>>,

    /// The last known ELR.
    ///
    /// Supported API versions: 0
    pub last_known_elr: Option<Vec<super::BrokerId>>,

    /// The set of offline replicas of this partition.
    ///
    /// Supported API versions: 0
    pub offline_replicas: Vec<super::BrokerId>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeTopicPartitionsResponsePartition {
    /// Sets `error_code` to the passed value.
    ///
    /// The partition error, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the leader broker.
    ///
    /// Supported API versions: 0
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 0
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    /// Sets `replica_nodes` to the passed value.
    ///
    /// The set of all nodes that host this partition.
    ///
    /// Supported API versions: 0
    pub fn with_replica_nodes(mut self, value: Vec<super::BrokerId>) -> Self {
        self.replica_nodes = value;
        self
    }
    /// Sets `isr_nodes` to the passed value.
    ///
    /// The set of nodes that are in sync with the leader for this partition.
    ///
    /// Supported API versions: 0
    pub fn with_isr_nodes(mut self, value: Vec<super::BrokerId>) -> Self {
        self.isr_nodes = value;
        self
    }
    /// Sets `eligible_leader_replicas` to the passed value.
    ///
    /// The new eligible leader replicas otherwise.
    ///
    /// Supported API versions: 0
    pub fn with_eligible_leader_replicas(mut self, value: Option<Vec<super::BrokerId>>) -> Self {
        self.eligible_leader_replicas = value;
        self
    }
    /// Sets `last_known_elr` to the passed value.
    ///
    /// The last known ELR.
    ///
    /// Supported API versions: 0
    pub fn with_last_known_elr(mut self, value: Option<Vec<super::BrokerId>>) -> Self {
        self.last_known_elr = value;
        self
    }
    /// Sets `offline_replicas` to the passed value.
    ///
    /// The set of offline replicas of this partition.
    ///
    /// Supported API versions: 0
    pub fn with_offline_replicas(mut self, value: Vec<super::BrokerId>) -> Self {
        self.offline_replicas = value;
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
impl Encodable for DescribeTopicPartitionsResponsePartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int32.encode(buf, &self.leader_id)?;
        types::Int32.encode(buf, &self.leader_epoch)?;
        types::CompactArray(types::Int32).encode(buf, &self.replica_nodes)?;
        types::CompactArray(types::Int32).encode(buf, &self.isr_nodes)?;
        types::CompactArray(types::Int32).encode(buf, &self.eligible_leader_replicas)?;
        types::CompactArray(types::Int32).encode(buf, &self.last_known_elr)?;
        types::CompactArray(types::Int32).encode(buf, &self.offline_replicas)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int32.compute_size(&self.leader_id)?;
        total_size += types::Int32.compute_size(&self.leader_epoch)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.replica_nodes)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.isr_nodes)?;
        total_size +=
            types::CompactArray(types::Int32).compute_size(&self.eligible_leader_replicas)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.last_known_elr)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.offline_replicas)?;
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
impl Decodable for DescribeTopicPartitionsResponsePartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let partition_index = types::Int32.decode(buf)?;
        let leader_id = types::Int32.decode(buf)?;
        let leader_epoch = types::Int32.decode(buf)?;
        let replica_nodes = types::CompactArray(types::Int32).decode(buf)?;
        let isr_nodes = types::CompactArray(types::Int32).decode(buf)?;
        let eligible_leader_replicas = types::CompactArray(types::Int32).decode(buf)?;
        let last_known_elr = types::CompactArray(types::Int32).decode(buf)?;
        let offline_replicas = types::CompactArray(types::Int32).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeTopicPartitionsResponsePartition {
    fn default() -> Self {
        Self {
            error_code: 0,
            partition_index: 0,
            leader_id: (0).into(),
            leader_epoch: -1,
            replica_nodes: Default::default(),
            isr_nodes: Default::default(),
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeTopicPartitionsResponsePartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The topic name.
    ///
    /// Supported API versions: 0
    pub name: Option<super::TopicName>,

    /// The topic id.
    ///
    /// Supported API versions: 0
    pub topic_id: Uuid,

    /// True if the topic is internal.
    ///
    /// Supported API versions: 0
    pub is_internal: bool,

    /// Each partition in the topic.
    ///
    /// Supported API versions: 0
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,

    /// 32-bit bitfield to represent authorized operations for this topic.
    ///
    /// Supported API versions: 0
    pub topic_authorized_operations: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeTopicPartitionsResponseTopic {
    /// Sets `error_code` to the passed value.
    ///
    /// The topic error, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: Option<super::TopicName>) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic id.
    ///
    /// Supported API versions: 0
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `is_internal` to the passed value.
    ///
    /// True if the topic is internal.
    ///
    /// Supported API versions: 0
    pub fn with_is_internal(mut self, value: bool) -> Self {
        self.is_internal = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// Each partition in the topic.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<DescribeTopicPartitionsResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
    /// Sets `topic_authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this topic.
    ///
    /// Supported API versions: 0
    pub fn with_topic_authorized_operations(mut self, value: i32) -> Self {
        self.topic_authorized_operations = value;
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
impl Encodable for DescribeTopicPartitionsResponseTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.name)?;
        types::Uuid.encode(buf, &self.topic_id)?;
        types::Boolean.encode(buf, &self.is_internal)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        types::Int32.encode(buf, &self.topic_authorized_operations)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Uuid.compute_size(&self.topic_id)?;
        total_size += types::Boolean.compute_size(&self.is_internal)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        total_size += types::Int32.compute_size(&self.topic_authorized_operations)?;
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
impl Decodable for DescribeTopicPartitionsResponseTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let name = types::CompactString.decode(buf)?;
        let topic_id = types::Uuid.decode(buf)?;
        let is_internal = types::Boolean.decode(buf)?;
        let partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let topic_authorized_operations = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeTopicPartitionsResponseTopic {
    fn default() -> Self {
        Self {
            error_code: 0,
            name: Some(Default::default()),
            topic_id: Uuid::nil(),
            is_internal: false,
            partitions: Default::default(),
            topic_authorized_operations: -2147483648,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeTopicPartitionsResponseTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeTopicPartitionsResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
