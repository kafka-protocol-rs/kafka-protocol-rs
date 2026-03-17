//! OffsetDeleteRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/OffsetDeleteRequest.json).
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
pub struct OffsetDeleteRequest {
    /// The unique group identifier.
    ///
    /// Supported API versions: 0
    pub group_id: super::GroupId,

    /// The topics to delete offsets for.
    ///
    /// Supported API versions: 0
    pub topics: Vec<OffsetDeleteRequestTopic>,
}

impl OffsetDeleteRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The unique group identifier.
    ///
    /// Supported API versions: 0
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to delete offsets for.
    ///
    /// Supported API versions: 0
    pub fn with_topics(mut self, value: Vec<OffsetDeleteRequestTopic>) -> Self {
        self.topics = value;
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for OffsetDeleteRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::String.encode(buf, &self.group_id)?;
        types::Array(types::Struct { version }).encode(buf, &self.topics)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::String.compute_size(&self.group_id)?;
        total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;

        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for OffsetDeleteRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let group_id = types::String.decode(buf)?;
        let topics = types::Array(types::Struct { version }).decode(buf)?;
        Ok(Self { group_id, topics })
    }
}

impl Default for OffsetDeleteRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            topics: Default::default(),
        }
    }
}

impl Message for OffsetDeleteRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteRequestPartition {
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub partition_index: i32,
}

impl OffsetDeleteRequestPartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for OffsetDeleteRequestPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition_index)?;

        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for OffsetDeleteRequestPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        Ok(Self { partition_index })
    }
}

impl Default for OffsetDeleteRequestPartition {
    fn default() -> Self {
        Self { partition_index: 0 }
    }
}

impl Message for OffsetDeleteRequestPartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteRequestTopic {
    /// The topic name.
    ///
    /// Supported API versions: 0
    pub name: super::TopicName,

    /// Each partition to delete offsets for.
    ///
    /// Supported API versions: 0
    pub partitions: Vec<OffsetDeleteRequestPartition>,
}

impl OffsetDeleteRequestTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// Each partition to delete offsets for.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<OffsetDeleteRequestPartition>) -> Self {
        self.partitions = value;
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for OffsetDeleteRequestTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::String.encode(buf, &self.name)?;
        types::Array(types::Struct { version }).encode(buf, &self.partitions)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::String.compute_size(&self.name)?;
        total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;

        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for OffsetDeleteRequestTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::String.decode(buf)?;
        let partitions = types::Array(types::Struct { version }).decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Default for OffsetDeleteRequestTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: Default::default(),
        }
    }
}

impl Message for OffsetDeleteRequestTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for OffsetDeleteRequest {
    fn header_version(version: i16) -> i16 {
        1
    }
}
