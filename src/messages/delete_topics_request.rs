//! DeleteTopicsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DeleteTopicsRequest.json).
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

/// Valid versions: 1-6
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTopicState {
    /// The topic name.
    ///
    /// Supported API versions: 6
    pub name: Option<super::TopicName>,

    /// The unique topic ID.
    ///
    /// Supported API versions: 6
    pub topic_id: Uuid,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteTopicState {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 6
    pub fn with_name(mut self, value: Option<super::TopicName>) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 6
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
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
impl Encodable for DeleteTopicState {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            if !self.name.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            types::Uuid.encode(buf, &self.topic_id)?;
        } else {
            if &self.topic_id != &Uuid::nil() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
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
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            if !self.name.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        } else {
            if &self.topic_id != &Uuid::nil() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
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
impl Decodable for DeleteTopicState {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let topic_id = if version >= 6 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
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
            topic_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteTopicState {
    fn default() -> Self {
        Self {
            name: None,
            topic_id: Uuid::nil(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteTopicState {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-6
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTopicsRequest {
    /// The name or topic ID of the topic.
    ///
    /// Supported API versions: 6
    pub topics: Vec<DeleteTopicState>,

    /// The names of the topics to delete.
    ///
    /// Supported API versions: 1-5
    pub topic_names: Vec<super::TopicName>,

    /// The length of time in milliseconds to wait for the deletions to complete.
    ///
    /// Supported API versions: 1-6
    pub timeout_ms: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteTopicsRequest {
    /// Sets `topics` to the passed value.
    ///
    /// The name or topic ID of the topic.
    ///
    /// Supported API versions: 6
    pub fn with_topics(mut self, value: Vec<DeleteTopicState>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `topic_names` to the passed value.
    ///
    /// The names of the topics to delete.
    ///
    /// Supported API versions: 1-5
    pub fn with_topic_names(mut self, value: Vec<super::TopicName>) -> Self {
        self.topic_names = value;
        self
    }
    /// Sets `timeout_ms` to the passed value.
    ///
    /// The length of time in milliseconds to wait for the deletions to complete.
    ///
    /// Supported API versions: 1-6
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
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
impl Encodable for DeleteTopicsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        if version >= 6 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 5 {
            if version >= 4 {
                types::CompactArray(types::CompactString).encode(buf, &self.topic_names)?;
            } else {
                types::Array(types::String).encode(buf, &self.topic_names)?;
            }
        }
        types::Int32.encode(buf, &self.timeout_ms)?;
        if version >= 4 {
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
        if version >= 6 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 5 {
            if version >= 4 {
                total_size +=
                    types::CompactArray(types::CompactString).compute_size(&self.topic_names)?;
            } else {
                total_size += types::Array(types::String).compute_size(&self.topic_names)?;
            }
        }
        total_size += types::Int32.compute_size(&self.timeout_ms)?;
        if version >= 4 {
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
impl Decodable for DeleteTopicsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        let topics = if version >= 6 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let topic_names = if version <= 5 {
            if version >= 4 {
                types::CompactArray(types::CompactString).decode(buf)?
            } else {
                types::Array(types::String).decode(buf)?
            }
        } else {
            Default::default()
        };
        let timeout_ms = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            topics,
            topic_names,
            timeout_ms,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteTopicsRequest {
    fn default() -> Self {
        Self {
            topics: Default::default(),
            topic_names: Default::default(),
            timeout_ms: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteTopicsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DeleteTopicsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 4 {
            2
        } else {
            1
        }
    }
}
