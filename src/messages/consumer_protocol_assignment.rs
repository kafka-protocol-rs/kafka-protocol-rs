//! ConsumerProtocolAssignment
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json).
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

/// Valid versions: 0-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerProtocolAssignment {
    /// The list of topics and partitions assigned to this consumer.
    ///
    /// Supported API versions: 0-3
    pub assigned_partitions: Vec<TopicPartition>,

    /// User data.
    ///
    /// Supported API versions: 0-3
    pub user_data: Option<Bytes>,
}

impl ConsumerProtocolAssignment {
    /// Sets `assigned_partitions` to the passed value.
    ///
    /// The list of topics and partitions assigned to this consumer.
    ///
    /// Supported API versions: 0-3
    pub fn with_assigned_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.assigned_partitions = value;
        self
    }
    /// Sets `user_data` to the passed value.
    ///
    /// User data.
    ///
    /// Supported API versions: 0-3
    pub fn with_user_data(mut self, value: Option<Bytes>) -> Self {
        self.user_data = value;
        self
    }
}

impl Encodable for ConsumerProtocolAssignment {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Array(types::Struct { version }).encode(buf, &self.assigned_partitions)?;
        types::Bytes.encode(buf, &self.user_data)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size +=
            types::Array(types::Struct { version }).compute_size(&self.assigned_partitions)?;
        total_size += types::Bytes.compute_size(&self.user_data)?;

        Ok(total_size)
    }
}

impl Decodable for ConsumerProtocolAssignment {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let assigned_partitions = types::Array(types::Struct { version }).decode(buf)?;
        let user_data = types::Bytes.decode(buf)?;
        Ok(Self {
            assigned_partitions,
            user_data,
        })
    }
}

impl Default for ConsumerProtocolAssignment {
    fn default() -> Self {
        Self {
            assigned_partitions: Default::default(),
            user_data: None,
        }
    }
}

impl Message for ConsumerProtocolAssignment {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    ///
    /// Supported API versions: 0-3
    pub topic: super::TopicName,

    /// The list of partitions assigned to this consumer.
    ///
    /// Supported API versions: 0-3
    pub partitions: Vec<i32>,
}

impl TopicPartition {
    /// Sets `topic` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0-3
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The list of partitions assigned to this consumer.
    ///
    /// Supported API versions: 0-3
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
}

impl Encodable for TopicPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::String.encode(buf, &self.topic)?;
        types::Array(types::Int32).encode(buf, &self.partitions)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::String.compute_size(&self.topic)?;
        total_size += types::Array(types::Int32).compute_size(&self.partitions)?;

        Ok(total_size)
    }
}

impl Decodable for TopicPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let topic = types::String.decode(buf)?;
        let partitions = types::Array(types::Int32).decode(buf)?;
        Ok(Self { topic, partitions })
    }
}

impl Default for TopicPartition {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            partitions: Default::default(),
        }
    }
}

impl Message for TopicPartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
