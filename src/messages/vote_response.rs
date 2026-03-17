//! VoteResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/VoteResponse.json).
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
pub struct NodeEndpoint {
    /// The ID of the associated node.
    ///
    /// Supported API versions: 1-2
    pub node_id: super::BrokerId,

    /// The node's hostname.
    ///
    /// Supported API versions: 1-2
    pub host: StrBytes,

    /// The node's port.
    ///
    /// Supported API versions: 1-2
    pub port: u16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl NodeEndpoint {
    /// Sets `node_id` to the passed value.
    ///
    /// The ID of the associated node.
    ///
    /// Supported API versions: 1-2
    pub fn with_node_id(mut self, value: super::BrokerId) -> Self {
        self.node_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The node's hostname.
    ///
    /// Supported API versions: 1-2
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The node's port.
    ///
    /// Supported API versions: 1-2
    pub fn with_port(mut self, value: u16) -> Self {
        self.port = value;
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
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::CompactString.encode(buf, &self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::UInt16.encode(buf, &self.port)?;
        } else {
            if self.port != 0 {
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
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::CompactString.compute_size(&self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::UInt16.compute_size(&self.port)?;
        } else {
            if self.port != 0 {
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

#[cfg(feature = "client")]
impl Decodable for NodeEndpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let node_id = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            (0).into()
        };
        let host = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let port = if version >= 1 {
            types::UInt16.decode(buf)?
        } else {
            0
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
            node_id,
            host,
            port,
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
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for NodeEndpoint {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 0-2
    pub partition_index: i32,

    /// The partition level error code.
    ///
    /// Supported API versions: 0-2
    pub error_code: i16,

    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 0-2
    pub leader_id: super::BrokerId,

    /// The latest known leader epoch.
    ///
    /// Supported API versions: 0-2
    pub leader_epoch: i32,

    /// True if the vote was granted and false otherwise.
    ///
    /// Supported API versions: 0-2
    pub vote_granted: bool,

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
    /// Sets `error_code` to the passed value.
    ///
    /// The partition level error code.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 0-2
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The latest known leader epoch.
    ///
    /// Supported API versions: 0-2
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    /// Sets `vote_granted` to the passed value.
    ///
    /// True if the vote was granted and false otherwise.
    ///
    /// Supported API versions: 0-2
    pub fn with_vote_granted(mut self, value: bool) -> Self {
        self.vote_granted = value;
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
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Int32.encode(buf, &self.leader_id)?;
        types::Int32.encode(buf, &self.leader_epoch)?;
        types::Boolean.encode(buf, &self.vote_granted)?;
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
        total_size += types::Int32.compute_size(&self.leader_id)?;
        total_size += types::Int32.compute_size(&self.leader_epoch)?;
        total_size += types::Boolean.compute_size(&self.vote_granted)?;
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
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let leader_id = types::Int32.decode(buf)?;
        let leader_epoch = types::Int32.decode(buf)?;
        let vote_granted = types::Boolean.decode(buf)?;
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
            leader_id,
            leader_epoch,
            vote_granted,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            error_code: 0,
            leader_id: (0).into(),
            leader_epoch: 0,
            vote_granted: false,
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

    /// The results for each partition.
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
    /// The results for each partition.
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

#[cfg(feature = "broker")]
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

#[cfg(feature = "client")]
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
pub struct VoteResponse {
    /// The top level error code.
    ///
    /// Supported API versions: 0-2
    pub error_code: i16,

    /// The results for each topic.
    ///
    /// Supported API versions: 0-2
    pub topics: Vec<TopicData>,

    /// Endpoints for all current-leaders enumerated in PartitionData.
    ///
    /// Supported API versions: 1-2
    pub node_endpoints: Vec<NodeEndpoint>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl VoteResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The top level error code.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The results for each topic.
    ///
    /// Supported API versions: 0-2
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `node_endpoints` to the passed value.
    ///
    /// Endpoints for all current-leaders enumerated in PartitionData.
    ///
    /// Supported API versions: 1-2
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
impl Encodable for VoteResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if !self.node_endpoints.is_empty() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
        if version >= 1 {
            if !self.node_endpoints.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.node_endpoints)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::CompactArray(types::Struct { version }).encode(buf, &self.node_endpoints)?;
            }
        }
        write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if !self.node_endpoints.is_empty() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
        if version >= 1 {
            if !self.node_endpoints.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.node_endpoints)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(0)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }
        }
        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for VoteResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut node_endpoints = Default::default();
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            match tag {
                0 => {
                    if version >= 1 {
                        node_endpoints =
                            types::CompactArray(types::Struct { version }).decode(buf)?;
                    } else {
                        bail!("Tag {} is not valid for version {}", tag, version);
                    }
                }
                _ => {
                    let unknown_value = buf.try_get_bytes(size as usize)?;
                    unknown_tagged_fields.insert(tag as i32, unknown_value);
                }
            }
        }
        Ok(Self {
            error_code,
            topics,
            node_endpoints,
            unknown_tagged_fields,
        })
    }
}

impl Default for VoteResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            topics: Default::default(),
            node_endpoints: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for VoteResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for VoteResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
