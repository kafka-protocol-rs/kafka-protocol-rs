//! FetchResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchResponse.json).
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

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    ///
    /// Supported API versions: 4-18
    pub producer_id: super::ProducerId,

    /// The first offset in the aborted transaction.
    ///
    /// Supported API versions: 4-18
    pub first_offset: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AbortedTransaction {
    /// Sets `producer_id` to the passed value.
    ///
    /// The producer id associated with the aborted transaction.
    ///
    /// Supported API versions: 4-18
    pub fn with_producer_id(mut self, value: super::ProducerId) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `first_offset` to the passed value.
    ///
    /// The first offset in the aborted transaction.
    ///
    /// Supported API versions: 4-18
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
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
impl Encodable for AbortedTransaction {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.producer_id)?;
        types::Int64.encode(buf, &self.first_offset)?;
        if version >= 12 {
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
        total_size += types::Int64.compute_size(&self.producer_id)?;
        total_size += types::Int64.compute_size(&self.first_offset)?;
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for AbortedTransaction {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let producer_id = types::Int64.decode(buf)?;
        let first_offset = types::Int64.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            producer_id,
            first_offset,
            unknown_tagged_fields,
        })
    }
}

impl Default for AbortedTransaction {
    fn default() -> Self {
        Self {
            producer_id: (0).into(),
            first_offset: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AbortedTransaction {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct EpochEndOffset {
    /// The largest epoch.
    ///
    /// Supported API versions: 12-18
    pub epoch: i32,

    /// The end offset of the epoch.
    ///
    /// Supported API versions: 12-18
    pub end_offset: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl EpochEndOffset {
    /// Sets `epoch` to the passed value.
    ///
    /// The largest epoch.
    ///
    /// Supported API versions: 12-18
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
        self
    }
    /// Sets `end_offset` to the passed value.
    ///
    /// The end offset of the epoch.
    ///
    /// Supported API versions: 12-18
    pub fn with_end_offset(mut self, value: i64) -> Self {
        self.end_offset = value;
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
impl Encodable for EpochEndOffset {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version >= 12 {
            types::Int32.encode(buf, &self.epoch)?;
        } else {
            if self.epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            types::Int64.encode(buf, &self.end_offset)?;
        } else {
            if self.end_offset != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
        if version >= 12 {
            total_size += types::Int32.compute_size(&self.epoch)?;
        } else {
            if self.epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            total_size += types::Int64.compute_size(&self.end_offset)?;
        } else {
            if self.end_offset != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for EpochEndOffset {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let epoch = if version >= 12 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let end_offset = if version >= 12 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            epoch,
            end_offset,
            unknown_tagged_fields,
        })
    }
}

impl Default for EpochEndOffset {
    fn default() -> Self {
        Self {
            epoch: -1,
            end_offset: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for EpochEndOffset {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 4-18
    pub throttle_time_ms: i32,

    /// The top level response error code.
    ///
    /// Supported API versions: 7-18
    pub error_code: i16,

    /// The fetch session ID, or 0 if this is not part of a fetch session.
    ///
    /// Supported API versions: 7-18
    pub session_id: i32,

    /// The response topics.
    ///
    /// Supported API versions: 4-18
    pub responses: Vec<FetchableTopicResponse>,

    /// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    ///
    /// Supported API versions: 16-18
    pub node_endpoints: Vec<NodeEndpoint>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 4-18
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top level response error code.
    ///
    /// Supported API versions: 7-18
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `session_id` to the passed value.
    ///
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    ///
    /// Supported API versions: 7-18
    pub fn with_session_id(mut self, value: i32) -> Self {
        self.session_id = value;
        self
    }
    /// Sets `responses` to the passed value.
    ///
    /// The response topics.
    ///
    /// Supported API versions: 4-18
    pub fn with_responses(mut self, value: Vec<FetchableTopicResponse>) -> Self {
        self.responses = value;
        self
    }
    /// Sets `node_endpoints` to the passed value.
    ///
    /// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    ///
    /// Supported API versions: 16-18
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
impl Encodable for FetchResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        if version >= 7 {
            types::Int16.encode(buf, &self.error_code)?;
        }
        if version >= 7 {
            types::Int32.encode(buf, &self.session_id)?;
        } else {
            if self.session_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.responses)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.responses)?;
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 16 {
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
            if version >= 16 {
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
                    types::CompactArray(types::Struct { version })
                        .encode(buf, &self.node_endpoints)?;
                }
            }
            write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        if version >= 7 {
            total_size += types::Int16.compute_size(&self.error_code)?;
        }
        if version >= 7 {
            total_size += types::Int32.compute_size(&self.session_id)?;
        } else {
            if self.session_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.responses)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.responses)?;
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 16 {
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
            if version >= 16 {
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
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for FetchResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = if version >= 7 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let session_id = if version >= 7 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let responses = if version >= 12 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut node_endpoints = Default::default();
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        if version >= 16 {
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
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: Default::default(),
            node_endpoints: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchResponse {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    ///
    /// Supported API versions: 4-12
    pub topic: super::TopicName,

    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub topic_id: Uuid,

    /// The topic partitions.
    ///
    /// Supported API versions: 4-18
    pub partitions: Vec<PartitionData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchableTopicResponse {
    /// Sets `topic` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 4-12
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The topic partitions.
    ///
    /// Supported API versions: 4-18
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
impl Encodable for FetchableTopicResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version <= 12 {
            if version >= 12 {
                types::CompactString.encode(buf, &self.topic)?;
            } else {
                types::String.encode(buf, &self.topic)?;
            }
        }
        if version >= 13 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 12 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
        }
        if version >= 12 {
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
        if version <= 12 {
            if version >= 12 {
                total_size += types::CompactString.compute_size(&self.topic)?;
            } else {
                total_size += types::String.compute_size(&self.topic)?;
            }
        }
        if version >= 13 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 12 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;
        }
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for FetchableTopicResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let topic = if version <= 12 {
            if version >= 12 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let topic_id = if version >= 13 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let partitions = if version >= 12 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchableTopicResponse {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchableTopicResponse {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 12-18
    pub leader_id: super::BrokerId,

    /// The latest known leader epoch.
    ///
    /// Supported API versions: 12-18
    pub leader_epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl LeaderIdAndEpoch {
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 12-18
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The latest known leader epoch.
    ///
    /// Supported API versions: 12-18
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
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version >= 12 {
            types::Int32.encode(buf, &self.leader_id)?;
        } else {
            if self.leader_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            types::Int32.encode(buf, &self.leader_epoch)?;
        } else {
            if self.leader_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
        if version >= 12 {
            total_size += types::Int32.compute_size(&self.leader_id)?;
        } else {
            if self.leader_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            total_size += types::Int32.compute_size(&self.leader_epoch)?;
        } else {
            if self.leader_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for LeaderIdAndEpoch {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let leader_id = if version >= 12 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let leader_epoch = if version >= 12 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
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
            leader_id: (-1).into(),
            leader_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for LeaderIdAndEpoch {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    ///
    /// Supported API versions: 16-18
    pub node_id: super::BrokerId,

    /// The node's hostname.
    ///
    /// Supported API versions: 16-18
    pub host: StrBytes,

    /// The node's port.
    ///
    /// Supported API versions: 16-18
    pub port: i32,

    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 16-18
    pub rack: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl NodeEndpoint {
    /// Sets `node_id` to the passed value.
    ///
    /// The ID of the associated node.
    ///
    /// Supported API versions: 16-18
    pub fn with_node_id(mut self, value: super::BrokerId) -> Self {
        self.node_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The node's hostname.
    ///
    /// Supported API versions: 16-18
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The node's port.
    ///
    /// Supported API versions: 16-18
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 16-18
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
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version >= 16 {
            types::Int32.encode(buf, &self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            types::CompactString.encode(buf, &self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            types::Int32.encode(buf, &self.port)?;
        } else {
            if self.port != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            types::CompactString.encode(buf, &self.rack)?;
        } else {
            if !self.rack.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
        if version >= 16 {
            total_size += types::Int32.compute_size(&self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            total_size += types::CompactString.compute_size(&self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            total_size += types::Int32.compute_size(&self.port)?;
        } else {
            if self.port != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 16 {
            total_size += types::CompactString.compute_size(&self.rack)?;
        } else {
            if !self.rack.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for NodeEndpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let node_id = if version >= 16 {
            types::Int32.decode(buf)?
        } else {
            (0).into()
        };
        let host = if version >= 16 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let port = if version >= 16 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let rack = if version >= 16 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
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
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 4-18
    pub partition_index: i32,

    /// The error code, or 0 if there was no fetch error.
    ///
    /// Supported API versions: 4-18
    pub error_code: i16,

    /// The current high water mark.
    ///
    /// Supported API versions: 4-18
    pub high_watermark: i64,

    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
    ///
    /// Supported API versions: 4-18
    pub last_stable_offset: i64,

    /// The current log start offset.
    ///
    /// Supported API versions: 5-18
    pub log_start_offset: i64,

    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
    ///
    /// Supported API versions: 12-18
    pub diverging_epoch: EpochEndOffset,

    /// The current leader of the partition.
    ///
    /// Supported API versions: 12-18
    pub current_leader: LeaderIdAndEpoch,

    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    ///
    /// Supported API versions: 12-18
    pub snapshot_id: SnapshotId,

    /// The aborted transactions.
    ///
    /// Supported API versions: 4-18
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,

    /// The preferred read replica for the consumer to use on its next fetch request.
    ///
    /// Supported API versions: 11-18
    pub preferred_read_replica: super::BrokerId,

    /// The record data.
    ///
    /// Supported API versions: 4-18
    pub records: Option<Bytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionData {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 4-18
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no fetch error.
    ///
    /// Supported API versions: 4-18
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `high_watermark` to the passed value.
    ///
    /// The current high water mark.
    ///
    /// Supported API versions: 4-18
    pub fn with_high_watermark(mut self, value: i64) -> Self {
        self.high_watermark = value;
        self
    }
    /// Sets `last_stable_offset` to the passed value.
    ///
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
    ///
    /// Supported API versions: 4-18
    pub fn with_last_stable_offset(mut self, value: i64) -> Self {
        self.last_stable_offset = value;
        self
    }
    /// Sets `log_start_offset` to the passed value.
    ///
    /// The current log start offset.
    ///
    /// Supported API versions: 5-18
    pub fn with_log_start_offset(mut self, value: i64) -> Self {
        self.log_start_offset = value;
        self
    }
    /// Sets `diverging_epoch` to the passed value.
    ///
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
    ///
    /// Supported API versions: 12-18
    pub fn with_diverging_epoch(mut self, value: EpochEndOffset) -> Self {
        self.diverging_epoch = value;
        self
    }
    /// Sets `current_leader` to the passed value.
    ///
    /// The current leader of the partition.
    ///
    /// Supported API versions: 12-18
    pub fn with_current_leader(mut self, value: LeaderIdAndEpoch) -> Self {
        self.current_leader = value;
        self
    }
    /// Sets `snapshot_id` to the passed value.
    ///
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    ///
    /// Supported API versions: 12-18
    pub fn with_snapshot_id(mut self, value: SnapshotId) -> Self {
        self.snapshot_id = value;
        self
    }
    /// Sets `aborted_transactions` to the passed value.
    ///
    /// The aborted transactions.
    ///
    /// Supported API versions: 4-18
    pub fn with_aborted_transactions(mut self, value: Option<Vec<AbortedTransaction>>) -> Self {
        self.aborted_transactions = value;
        self
    }
    /// Sets `preferred_read_replica` to the passed value.
    ///
    /// The preferred read replica for the consumer to use on its next fetch request.
    ///
    /// Supported API versions: 11-18
    pub fn with_preferred_read_replica(mut self, value: super::BrokerId) -> Self {
        self.preferred_read_replica = value;
        self
    }
    /// Sets `records` to the passed value.
    ///
    /// The record data.
    ///
    /// Supported API versions: 4-18
    pub fn with_records(mut self, value: Option<Bytes>) -> Self {
        self.records = value;
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
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Int64.encode(buf, &self.high_watermark)?;
        types::Int64.encode(buf, &self.last_stable_offset)?;
        if version >= 5 {
            types::Int64.encode(buf, &self.log_start_offset)?;
        }
        if version >= 12 {
            types::CompactArray(types::Struct { version })
                .encode(buf, &self.aborted_transactions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.aborted_transactions)?;
        }
        if version >= 11 {
            types::Int32.encode(buf, &self.preferred_read_replica)?;
        } else {
            if self.preferred_read_replica != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            types::CompactBytes.encode(buf, &self.records)?;
        } else {
            types::Bytes.encode(buf, &self.records)?;
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if &self.diverging_epoch != &Default::default() {
                num_tagged_fields += 1;
            }
            if &self.current_leader != &Default::default() {
                num_tagged_fields += 1;
            }
            if &self.snapshot_id != &Default::default() {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
            if &self.diverging_epoch != &Default::default() {
                let computed_size =
                    types::Struct { version }.compute_size(&self.diverging_epoch)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Struct { version }.encode(buf, &self.diverging_epoch)?;
            }
            if &self.current_leader != &Default::default() {
                let computed_size = types::Struct { version }.compute_size(&self.current_leader)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 1)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Struct { version }.encode(buf, &self.current_leader)?;
            }
            if &self.snapshot_id != &Default::default() {
                let computed_size = types::Struct { version }.compute_size(&self.snapshot_id)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 2)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Struct { version }.encode(buf, &self.snapshot_id)?;
            }

            write_unknown_tagged_fields(buf, 3.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int64.compute_size(&self.high_watermark)?;
        total_size += types::Int64.compute_size(&self.last_stable_offset)?;
        if version >= 5 {
            total_size += types::Int64.compute_size(&self.log_start_offset)?;
        }
        if version >= 12 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.aborted_transactions)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.aborted_transactions)?;
        }
        if version >= 11 {
            total_size += types::Int32.compute_size(&self.preferred_read_replica)?;
        } else {
            if self.preferred_read_replica != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
            total_size += types::CompactBytes.compute_size(&self.records)?;
        } else {
            total_size += types::Bytes.compute_size(&self.records)?;
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if &self.diverging_epoch != &Default::default() {
                num_tagged_fields += 1;
            }
            if &self.current_leader != &Default::default() {
                num_tagged_fields += 1;
            }
            if &self.snapshot_id != &Default::default() {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
            if &self.diverging_epoch != &Default::default() {
                let computed_size =
                    types::Struct { version }.compute_size(&self.diverging_epoch)?;
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
            if &self.current_leader != &Default::default() {
                let computed_size = types::Struct { version }.compute_size(&self.current_leader)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(1)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }
            if &self.snapshot_id != &Default::default() {
                let computed_size = types::Struct { version }.compute_size(&self.snapshot_id)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(2)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for PartitionData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let high_watermark = types::Int64.decode(buf)?;
        let last_stable_offset = types::Int64.decode(buf)?;
        let log_start_offset = if version >= 5 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let mut diverging_epoch = Default::default();
        let mut current_leader = Default::default();
        let mut snapshot_id = Default::default();
        let aborted_transactions = if version >= 12 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let preferred_read_replica = if version >= 11 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let records = if version >= 12 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        diverging_epoch = types::Struct { version }.decode(buf)?;
                    }
                    1 => {
                        current_leader = types::Struct { version }.decode(buf)?;
                    }
                    2 => {
                        snapshot_id = types::Struct { version }.decode(buf)?;
                    }
                    _ => {
                        let unknown_value = buf.try_get_bytes(size as usize)?;
                        unknown_tagged_fields.insert(tag as i32, unknown_value);
                    }
                }
            }
        }
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            diverging_epoch,
            current_leader,
            snapshot_id,
            aborted_transactions,
            preferred_read_replica,
            records,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            error_code: 0,
            high_watermark: 0,
            last_stable_offset: -1,
            log_start_offset: -1,
            diverging_epoch: Default::default(),
            current_leader: Default::default(),
            snapshot_id: Default::default(),
            aborted_transactions: Some(Default::default()),
            preferred_read_replica: (-1).into(),
            records: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionData {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the epoch.
    ///
    /// Supported API versions: 4-18
    pub end_offset: i64,

    /// The largest epoch.
    ///
    /// Supported API versions: 4-18
    pub epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SnapshotId {
    /// Sets `end_offset` to the passed value.
    ///
    /// The end offset of the epoch.
    ///
    /// Supported API versions: 4-18
    pub fn with_end_offset(mut self, value: i64) -> Self {
        self.end_offset = value;
        self
    }
    /// Sets `epoch` to the passed value.
    ///
    /// The largest epoch.
    ///
    /// Supported API versions: 4-18
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
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
impl Encodable for SnapshotId {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.end_offset)?;
        types::Int32.encode(buf, &self.epoch)?;
        if version >= 12 {
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
        total_size += types::Int64.compute_size(&self.end_offset)?;
        total_size += types::Int32.compute_size(&self.epoch)?;
        if version >= 12 {
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

#[cfg(feature = "client")]
impl Decodable for SnapshotId {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let end_offset = types::Int64.decode(buf)?;
        let epoch = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            end_offset,
            epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self {
            end_offset: -1,
            epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SnapshotId {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for FetchResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 12 {
            1
        } else {
            0
        }
    }
}
