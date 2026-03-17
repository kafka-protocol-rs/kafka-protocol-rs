//! OffsetDeleteResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/OffsetDeleteResponse.json).
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
pub struct OffsetDeleteResponse {
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub throttle_time_ms: i32,

    /// The responses for each topic.
    ///
    /// Supported API versions: 0
    pub topics: Vec<OffsetDeleteResponseTopic>,
}

impl OffsetDeleteResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
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
    /// The responses for each topic.
    ///
    /// Supported API versions: 0
    pub fn with_topics(mut self, value: Vec<OffsetDeleteResponseTopic>) -> Self {
        self.topics = value;
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for OffsetDeleteResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Array(types::Struct { version }).encode(buf, &self.topics)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;

        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for OffsetDeleteResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let throttle_time_ms = types::Int32.decode(buf)?;
        let topics = types::Array(types::Struct { version }).decode(buf)?;
        Ok(Self {
            error_code,
            throttle_time_ms,
            topics,
        })
    }
}

impl Default for OffsetDeleteResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            throttle_time_ms: 0,
            topics: Default::default(),
        }
    }
}

impl Message for OffsetDeleteResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteResponsePartition {
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub partition_index: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,
}

impl OffsetDeleteResponsePartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
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
}

#[cfg(feature = "broker")]
impl Encodable for OffsetDeleteResponsePartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int16.encode(buf, &self.error_code)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int16.compute_size(&self.error_code)?;

        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for OffsetDeleteResponsePartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        Ok(Self {
            partition_index,
            error_code,
        })
    }
}

impl Default for OffsetDeleteResponsePartition {
    fn default() -> Self {
        Self {
            partition_index: 0,
            error_code: 0,
        }
    }
}

impl Message for OffsetDeleteResponsePartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetDeleteResponseTopic {
    /// The topic name.
    ///
    /// Supported API versions: 0
    pub name: super::TopicName,

    /// The responses for each partition in the topic.
    ///
    /// Supported API versions: 0
    pub partitions: Vec<OffsetDeleteResponsePartition>,
}

impl OffsetDeleteResponseTopic {
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
    /// The responses for each partition in the topic.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<OffsetDeleteResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for OffsetDeleteResponseTopic {
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

#[cfg(feature = "client")]
impl Decodable for OffsetDeleteResponseTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::String.decode(buf)?;
        let partitions = types::Array(types::Struct { version }).decode(buf)?;
        Ok(Self { name, partitions })
    }
}

impl Default for OffsetDeleteResponseTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: Default::default(),
        }
    }
}

impl Message for OffsetDeleteResponseTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for OffsetDeleteResponse {
    fn header_version(version: i16) -> i16 {
        0
    }
}
