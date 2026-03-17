//! EndQuorumEpochRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/EndQuorumEpochRequest.json).
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

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct EndQuorumEpochRequest {
    /// The cluster id.
    ///
    /// Supported API versions: 0-1
    pub cluster_id: Option<StrBytes>,

    /// The topics.
    ///
    /// Supported API versions: 0-1
    pub topics: Vec<TopicData>,

    /// Endpoints for the leader.
    ///
    /// Supported API versions: 1
    pub leader_endpoints: Vec<LeaderEndpoint>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl EndQuorumEpochRequest {
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster id.
    ///
    /// Supported API versions: 0-1
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics.
    ///
    /// Supported API versions: 0-1
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `leader_endpoints` to the passed value.
    ///
    /// Endpoints for the leader.
    ///
    /// Supported API versions: 1
    pub fn with_leader_endpoints(mut self, value: Vec<LeaderEndpoint>) -> Self {
        self.leader_endpoints = value;
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
impl Encodable for EndQuorumEpochRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::CompactString.encode(buf, &self.cluster_id)?;
        } else {
            types::String.encode(buf, &self.cluster_id)?;
        }
        if version >= 1 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 1 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.leader_endpoints)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 1 {
            total_size += types::CompactString.compute_size(&self.cluster_id)?;
        } else {
            total_size += types::String.compute_size(&self.cluster_id)?;
        }
        if version >= 1 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 1 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.leader_endpoints)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for EndQuorumEpochRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let cluster_id = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let topics = if version >= 1 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let leader_endpoints = if version >= 1 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            cluster_id,
            topics,
            leader_endpoints,
            unknown_tagged_fields,
        })
    }
}

impl Default for EndQuorumEpochRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            topics: Default::default(),
            leader_endpoints: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for EndQuorumEpochRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderEndpoint {
    /// The name of the endpoint.
    ///
    /// Supported API versions: 1
    pub name: StrBytes,

    /// The node's hostname.
    ///
    /// Supported API versions: 1
    pub host: StrBytes,

    /// The node's port.
    ///
    /// Supported API versions: 1
    pub port: u16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl LeaderEndpoint {
    /// Sets `name` to the passed value.
    ///
    /// The name of the endpoint.
    ///
    /// Supported API versions: 1
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
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

#[cfg(feature = "client")]
impl Encodable for LeaderEndpoint {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            if !self.name.is_empty() {
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
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 1 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            if !self.name.is_empty() {
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
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for LeaderEndpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
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
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            name,
            host,
            port,
            unknown_tagged_fields,
        })
    }
}

impl Default for LeaderEndpoint {
    fn default() -> Self {
        Self {
            name: Default::default(),
            host: Default::default(),
            port: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for LeaderEndpoint {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 0-1
    pub partition_index: i32,

    /// The current leader ID that is resigning.
    ///
    /// Supported API versions: 0-1
    pub leader_id: super::BrokerId,

    /// The current epoch.
    ///
    /// Supported API versions: 0-1
    pub leader_epoch: i32,

    /// A sorted list of preferred successors to start the election.
    ///
    /// Supported API versions: 0
    pub preferred_successors: Vec<i32>,

    /// A sorted list of preferred candidates to start the election.
    ///
    /// Supported API versions: 1
    pub preferred_candidates: Vec<ReplicaInfo>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionData {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0-1
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `leader_id` to the passed value.
    ///
    /// The current leader ID that is resigning.
    ///
    /// Supported API versions: 0-1
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The current epoch.
    ///
    /// Supported API versions: 0-1
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    /// Sets `preferred_successors` to the passed value.
    ///
    /// A sorted list of preferred successors to start the election.
    ///
    /// Supported API versions: 0
    pub fn with_preferred_successors(mut self, value: Vec<i32>) -> Self {
        self.preferred_successors = value;
        self
    }
    /// Sets `preferred_candidates` to the passed value.
    ///
    /// A sorted list of preferred candidates to start the election.
    ///
    /// Supported API versions: 1
    pub fn with_preferred_candidates(mut self, value: Vec<ReplicaInfo>) -> Self {
        self.preferred_candidates = value;
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
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int32.encode(buf, &self.leader_id)?;
        types::Int32.encode(buf, &self.leader_epoch)?;
        if version == 0 {
            types::Array(types::Int32).encode(buf, &self.preferred_successors)?;
        }
        if version >= 1 {
            types::CompactArray(types::Struct { version })
                .encode(buf, &self.preferred_candidates)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int32.compute_size(&self.leader_id)?;
        total_size += types::Int32.compute_size(&self.leader_epoch)?;
        if version == 0 {
            total_size += types::Array(types::Int32).compute_size(&self.preferred_successors)?;
        }
        if version >= 1 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.preferred_candidates)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for PartitionData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let leader_id = types::Int32.decode(buf)?;
        let leader_epoch = types::Int32.decode(buf)?;
        let preferred_successors = if version == 0 {
            types::Array(types::Int32).decode(buf)?
        } else {
            Default::default()
        };
        let preferred_candidates = if version >= 1 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            partition_index,
            leader_id,
            leader_epoch,
            preferred_successors,
            preferred_candidates,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            leader_id: (0).into(),
            leader_epoch: 0,
            preferred_successors: Default::default(),
            preferred_candidates: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionData {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaInfo {
    /// The ID of the candidate replica.
    ///
    /// Supported API versions: 1
    pub candidate_id: super::BrokerId,

    /// The directory ID of the candidate replica.
    ///
    /// Supported API versions: 1
    pub candidate_directory_id: Uuid,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ReplicaInfo {
    /// Sets `candidate_id` to the passed value.
    ///
    /// The ID of the candidate replica.
    ///
    /// Supported API versions: 1
    pub fn with_candidate_id(mut self, value: super::BrokerId) -> Self {
        self.candidate_id = value;
        self
    }
    /// Sets `candidate_directory_id` to the passed value.
    ///
    /// The directory ID of the candidate replica.
    ///
    /// Supported API versions: 1
    pub fn with_candidate_directory_id(mut self, value: Uuid) -> Self {
        self.candidate_directory_id = value;
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
impl Encodable for ReplicaInfo {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.candidate_id)?;
        } else {
            if self.candidate_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::Uuid.encode(buf, &self.candidate_directory_id)?;
        } else {
            if &self.candidate_directory_id != &Uuid::nil() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.candidate_id)?;
        } else {
            if self.candidate_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::Uuid.compute_size(&self.candidate_directory_id)?;
        } else {
            if &self.candidate_directory_id != &Uuid::nil() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for ReplicaInfo {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let candidate_id = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            (0).into()
        };
        let candidate_directory_id = if version >= 1 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            candidate_id,
            candidate_directory_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for ReplicaInfo {
    fn default() -> Self {
        Self {
            candidate_id: (0).into(),
            candidate_directory_id: Uuid::nil(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ReplicaInfo {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The topic name.
    ///
    /// Supported API versions: 0-1
    pub topic_name: super::TopicName,

    /// The partitions.
    ///
    /// Supported API versions: 0-1
    pub partitions: Vec<PartitionData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicData {
    /// Sets `topic_name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0-1
    pub fn with_topic_name(mut self, value: super::TopicName) -> Self {
        self.topic_name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions.
    ///
    /// Supported API versions: 0-1
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
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::CompactString.encode(buf, &self.topic_name)?;
        } else {
            types::String.encode(buf, &self.topic_name)?;
        }
        if version >= 1 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 1 {
            total_size += types::CompactString.compute_size(&self.topic_name)?;
        } else {
            total_size += types::String.compute_size(&self.topic_name)?;
        }
        if version >= 1 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;
        }
        if version >= 1 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopicData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let topic_name = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let partitions = if version >= 1 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
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
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for EndQuorumEpochRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 1 {
            2
        } else {
            1
        }
    }
}
