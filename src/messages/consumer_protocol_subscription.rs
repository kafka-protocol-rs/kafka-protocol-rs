//! ConsumerProtocolSubscription
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json).
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
pub struct ConsumerProtocolSubscription {
    /// The topics that the member wants to consume.
    ///
    /// Supported API versions: 0-3
    pub topics: Vec<StrBytes>,

    /// User data that will be passed back to the consumer.
    ///
    /// Supported API versions: 0-3
    pub user_data: Option<Bytes>,

    /// The partitions that the member owns.
    ///
    /// Supported API versions: 1-3
    pub owned_partitions: Vec<TopicPartition>,

    /// The generation id of the member.
    ///
    /// Supported API versions: 2-3
    pub generation_id: i32,

    /// The rack id of the member.
    ///
    /// Supported API versions: 3
    pub rack_id: Option<StrBytes>,
}

impl ConsumerProtocolSubscription {
    /// Sets `topics` to the passed value.
    ///
    /// The topics that the member wants to consume.
    ///
    /// Supported API versions: 0-3
    pub fn with_topics(mut self, value: Vec<StrBytes>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `user_data` to the passed value.
    ///
    /// User data that will be passed back to the consumer.
    ///
    /// Supported API versions: 0-3
    pub fn with_user_data(mut self, value: Option<Bytes>) -> Self {
        self.user_data = value;
        self
    }
    /// Sets `owned_partitions` to the passed value.
    ///
    /// The partitions that the member owns.
    ///
    /// Supported API versions: 1-3
    pub fn with_owned_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.owned_partitions = value;
        self
    }
    /// Sets `generation_id` to the passed value.
    ///
    /// The generation id of the member.
    ///
    /// Supported API versions: 2-3
    pub fn with_generation_id(mut self, value: i32) -> Self {
        self.generation_id = value;
        self
    }
    /// Sets `rack_id` to the passed value.
    ///
    /// The rack id of the member.
    ///
    /// Supported API versions: 3
    pub fn with_rack_id(mut self, value: Option<StrBytes>) -> Self {
        self.rack_id = value;
        self
    }
}

impl Encodable for ConsumerProtocolSubscription {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Array(types::String).encode(buf, &self.topics)?;
        types::Bytes.encode(buf, &self.user_data)?;
        if version >= 1 {
            types::Array(types::Struct { version }).encode(buf, &self.owned_partitions)?;
        }
        if version >= 2 {
            types::Int32.encode(buf, &self.generation_id)?;
        }
        if version >= 3 {
            types::String.encode(buf, &self.rack_id)?;
        }

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Array(types::String).compute_size(&self.topics)?;
        total_size += types::Bytes.compute_size(&self.user_data)?;
        if version >= 1 {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.owned_partitions)?;
        }
        if version >= 2 {
            total_size += types::Int32.compute_size(&self.generation_id)?;
        }
        if version >= 3 {
            total_size += types::String.compute_size(&self.rack_id)?;
        }

        Ok(total_size)
    }
}

impl Decodable for ConsumerProtocolSubscription {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let topics = types::Array(types::String).decode(buf)?;
        let user_data = types::Bytes.decode(buf)?;
        let owned_partitions = if version >= 1 {
            types::Array(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let generation_id = if version >= 2 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let rack_id = if version >= 3 {
            types::String.decode(buf)?
        } else {
            None
        };
        Ok(Self {
            topics,
            user_data,
            owned_partitions,
            generation_id,
            rack_id,
        })
    }
}

impl Default for ConsumerProtocolSubscription {
    fn default() -> Self {
        Self {
            topics: Default::default(),
            user_data: None,
            owned_partitions: Default::default(),
            generation_id: -1,
            rack_id: None,
        }
    }
}

impl Message for ConsumerProtocolSubscription {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    ///
    /// Supported API versions: 1-3
    pub topic: super::TopicName,

    /// The partition ids.
    ///
    /// Supported API versions: 1-3
    pub partitions: Vec<i32>,
}

impl TopicPartition {
    /// Sets `topic` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 1-3
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partition ids.
    ///
    /// Supported API versions: 1-3
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
        if version >= 1 {
            types::String.encode(buf, &self.topic)?;
        } else {
            if !self.topic.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::Array(types::Int32).encode(buf, &self.partitions)?;
        } else {
            if !self.partitions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 1 {
            total_size += types::String.compute_size(&self.topic)?;
        } else {
            if !self.topic.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::Array(types::Int32).compute_size(&self.partitions)?;
        } else {
            if !self.partitions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }

        Ok(total_size)
    }
}

impl Decodable for TopicPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let topic = if version >= 1 {
            types::String.decode(buf)?
        } else {
            Default::default()
        };
        let partitions = if version >= 1 {
            types::Array(types::Int32).decode(buf)?
        } else {
            Default::default()
        };
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
