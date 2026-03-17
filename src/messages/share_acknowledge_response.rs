//! ShareAcknowledgeResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ShareAcknowledgeResponse.json).
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
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 1
    pub leader_id: i32,

    /// The latest known leader epoch.
    ///
    /// Supported API versions: 1
    pub leader_epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl LeaderIdAndEpoch {
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 1
    pub fn with_leader_id(mut self, value: i32) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The latest known leader epoch.
    ///
    /// Supported API versions: 1
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
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
impl Encodable for LeaderIdAndEpoch {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.leader_id)?;
        types::Int32.encode(buf, &self.leader_epoch)?;
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
        total_size += types::Int32.compute_size(&self.leader_id)?;
        total_size += types::Int32.compute_size(&self.leader_epoch)?;
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
impl Decodable for LeaderIdAndEpoch {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let leader_id = types::Int32.decode(buf)?;
        let leader_epoch = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            leader_id,
            leader_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for LeaderIdAndEpoch {
    fn default() -> Self {
        Self {
            leader_id: 0,
            leader_epoch: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for LeaderIdAndEpoch {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    ///
    /// Supported API versions: 1
    pub node_id: super::BrokerId,

    /// The node's hostname.
    ///
    /// Supported API versions: 1
    pub host: StrBytes,

    /// The node's port.
    ///
    /// Supported API versions: 1
    pub port: i32,

    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 1
    pub rack: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl NodeEndpoint {
    /// Sets `node_id` to the passed value.
    ///
    /// The ID of the associated node.
    ///
    /// Supported API versions: 1
    pub fn with_node_id(mut self, value: super::BrokerId) -> Self {
        self.node_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The node's hostname.
    ///
    /// Supported API versions: 1
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The node's port.
    ///
    /// Supported API versions: 1
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 1
    pub fn with_rack(mut self, value: Option<StrBytes>) -> Self {
        self.rack = value;
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
impl Encodable for NodeEndpoint {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.node_id)?;
        types::CompactString.encode(buf, &self.host)?;
        types::Int32.encode(buf, &self.port)?;
        types::CompactString.encode(buf, &self.rack)?;
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
        total_size += types::Int32.compute_size(&self.node_id)?;
        total_size += types::CompactString.compute_size(&self.host)?;
        total_size += types::Int32.compute_size(&self.port)?;
        total_size += types::CompactString.compute_size(&self.rack)?;
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
impl Decodable for NodeEndpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let node_id = types::Int32.decode(buf)?;
        let host = types::CompactString.decode(buf)?;
        let port = types::Int32.decode(buf)?;
        let rack = types::CompactString.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            node_id,
            host,
            port,
            rack,
            unknown_tagged_fields,
        })
    }
}

impl Default for NodeEndpoint {
    fn default() -> Self {
        Self {
            node_id: (0).into(),
            host: Default::default(),
            port: 0,
            rack: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for NodeEndpoint {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub partition_index: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1
    pub error_code: i16,

    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub error_message: Option<StrBytes>,

    /// The current leader of the partition.
    ///
    /// Supported API versions: 1
    pub current_leader: LeaderIdAndEpoch,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionData {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 1
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `current_leader` to the passed value.
    ///
    /// The current leader of the partition.
    ///
    /// Supported API versions: 1
    pub fn with_current_leader(mut self, value: LeaderIdAndEpoch) -> Self {
        self.current_leader = value;
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
impl Encodable for PartitionData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::Struct { version }.encode(buf, &self.current_leader)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::Struct { version }.compute_size(&self.current_leader)?;
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
impl Decodable for PartitionData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let current_leader = types::Struct { version }.decode(buf)?;
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
            error_code,
            error_message,
            current_leader,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            error_code: 0,
            error_message: None,
            current_leader: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionData {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ShareAcknowledgeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1
    pub throttle_time_ms: i32,

    /// The top level response error code.
    ///
    /// Supported API versions: 1
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub error_message: Option<StrBytes>,

    /// The response topics.
    ///
    /// Supported API versions: 1
    pub responses: Vec<ShareAcknowledgeTopicResponse>,

    /// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
    ///
    /// Supported API versions: 1
    pub node_endpoints: Vec<NodeEndpoint>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareAcknowledgeResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top level response error code.
    ///
    /// Supported API versions: 1
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `responses` to the passed value.
    ///
    /// The response topics.
    ///
    /// Supported API versions: 1
    pub fn with_responses(mut self, value: Vec<ShareAcknowledgeTopicResponse>) -> Self {
        self.responses = value;
        self
    }
    /// Sets `node_endpoints` to the passed value.
    ///
    /// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
    ///
    /// Supported API versions: 1
    pub fn with_node_endpoints(mut self, value: Vec<NodeEndpoint>) -> Self {
        self.node_endpoints = value;
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
impl Encodable for ShareAcknowledgeResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.responses)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.node_endpoints)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.responses)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.node_endpoints)?;
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
impl Decodable for ShareAcknowledgeResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let responses = types::CompactArray(types::Struct { version }).decode(buf)?;
        let node_endpoints = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            error_code,
            error_message,
            responses,
            node_endpoints,
            unknown_tagged_fields,
        })
    }
}

impl Default for ShareAcknowledgeResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            responses: Default::default(),
            node_endpoints: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareAcknowledgeResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ShareAcknowledgeTopicResponse {
    /// The unique topic ID.
    ///
    /// Supported API versions: 1
    pub topic_id: Uuid,

    /// The topic partitions.
    ///
    /// Supported API versions: 1
    pub partitions: Vec<PartitionData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareAcknowledgeTopicResponse {
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
    /// The topic partitions.
    ///
    /// Supported API versions: 1
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

#[cfg(feature = "broker")]
impl Encodable for ShareAcknowledgeTopicResponse {
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

#[cfg(feature = "client")]
impl Decodable for ShareAcknowledgeTopicResponse {
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

impl Default for ShareAcknowledgeTopicResponse {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareAcknowledgeTopicResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ShareAcknowledgeResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
