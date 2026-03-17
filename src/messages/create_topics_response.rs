//! CreateTopicsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/CreateTopicsResponse.json).
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

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    ///
    /// Supported API versions: 5-7
    pub name: StrBytes,

    /// The configuration value.
    ///
    /// Supported API versions: 5-7
    pub value: Option<StrBytes>,

    /// True if the configuration is read-only.
    ///
    /// Supported API versions: 5-7
    pub read_only: bool,

    /// The configuration source.
    ///
    /// Supported API versions: 5-7
    pub config_source: i8,

    /// True if this configuration is sensitive.
    ///
    /// Supported API versions: 5-7
    pub is_sensitive: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreatableTopicConfigs {
    /// Sets `name` to the passed value.
    ///
    /// The configuration name.
    ///
    /// Supported API versions: 5-7
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `value` to the passed value.
    ///
    /// The configuration value.
    ///
    /// Supported API versions: 5-7
    pub fn with_value(mut self, value: Option<StrBytes>) -> Self {
        self.value = value;
        self
    }
    /// Sets `read_only` to the passed value.
    ///
    /// True if the configuration is read-only.
    ///
    /// Supported API versions: 5-7
    pub fn with_read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    /// Sets `config_source` to the passed value.
    ///
    /// The configuration source.
    ///
    /// Supported API versions: 5-7
    pub fn with_config_source(mut self, value: i8) -> Self {
        self.config_source = value;
        self
    }
    /// Sets `is_sensitive` to the passed value.
    ///
    /// True if this configuration is sensitive.
    ///
    /// Supported API versions: 5-7
    pub fn with_is_sensitive(mut self, value: bool) -> Self {
        self.is_sensitive = value;
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
impl Encodable for CreatableTopicConfigs {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.value)?;
        } else {
            if !self
                .value
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            types::Boolean.encode(buf, &self.read_only)?;
        } else {
            if self.read_only {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            types::Int8.encode(buf, &self.config_source)?;
        }
        if version >= 5 {
            types::Boolean.encode(buf, &self.is_sensitive)?;
        } else {
            if self.is_sensitive {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
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
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.value)?;
        } else {
            if !self
                .value
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            total_size += types::Boolean.compute_size(&self.read_only)?;
        } else {
            if self.read_only {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            total_size += types::Int8.compute_size(&self.config_source)?;
        }
        if version >= 5 {
            total_size += types::Boolean.compute_size(&self.is_sensitive)?;
        } else {
            if self.is_sensitive {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
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
impl Decodable for CreatableTopicConfigs {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let value = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            Some(Default::default())
        };
        let read_only = if version >= 5 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let config_source = if version >= 5 {
            types::Int8.decode(buf)?
        } else {
            -1
        };
        let is_sensitive = if version >= 5 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 5 {
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
            value,
            read_only,
            config_source,
            is_sensitive,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreatableTopicConfigs {
    fn default() -> Self {
        Self {
            name: Default::default(),
            value: Some(Default::default()),
            read_only: false,
            config_source: -1,
            is_sensitive: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreatableTopicConfigs {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopicResult {
    /// The topic name.
    ///
    /// Supported API versions: 2-7
    pub name: super::TopicName,

    /// The unique topic ID.
    ///
    /// Supported API versions: 7
    pub topic_id: Uuid,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 2-7
    pub error_code: i16,

    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 2-7
    pub error_message: Option<StrBytes>,

    /// Optional topic config error returned if configs are not returned in the response.
    ///
    /// Supported API versions: 5-7
    pub topic_config_error_code: i16,

    /// Number of partitions of the topic.
    ///
    /// Supported API versions: 5-7
    pub num_partitions: i32,

    /// Replication factor of the topic.
    ///
    /// Supported API versions: 5-7
    pub replication_factor: i16,

    /// Configuration of the topic.
    ///
    /// Supported API versions: 5-7
    pub configs: Option<Vec<CreatableTopicConfigs>>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreatableTopicResult {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 2-7
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 7
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 2-7
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 2-7
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `topic_config_error_code` to the passed value.
    ///
    /// Optional topic config error returned if configs are not returned in the response.
    ///
    /// Supported API versions: 5-7
    pub fn with_topic_config_error_code(mut self, value: i16) -> Self {
        self.topic_config_error_code = value;
        self
    }
    /// Sets `num_partitions` to the passed value.
    ///
    /// Number of partitions of the topic.
    ///
    /// Supported API versions: 5-7
    pub fn with_num_partitions(mut self, value: i32) -> Self {
        self.num_partitions = value;
        self
    }
    /// Sets `replication_factor` to the passed value.
    ///
    /// Replication factor of the topic.
    ///
    /// Supported API versions: 5-7
    pub fn with_replication_factor(mut self, value: i16) -> Self {
        self.replication_factor = value;
        self
    }
    /// Sets `configs` to the passed value.
    ///
    /// Configuration of the topic.
    ///
    /// Supported API versions: 5-7
    pub fn with_configs(mut self, value: Option<Vec<CreatableTopicConfigs>>) -> Self {
        self.configs = value;
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
impl Encodable for CreatableTopicResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 7 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 5 {
            types::CompactString.encode(buf, &self.error_message)?;
        } else {
            types::String.encode(buf, &self.error_message)?;
        }
        if version >= 5 {
            types::Int32.encode(buf, &self.num_partitions)?;
        }
        if version >= 5 {
            types::Int16.encode(buf, &self.replication_factor)?;
        }
        if version >= 5 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.configs)?;
        }
        if version >= 5 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if self.topic_config_error_code != 0 {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
            if self.topic_config_error_code != 0 {
                let computed_size = types::Int16.compute_size(&self.topic_config_error_code)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Int16.encode(buf, &self.topic_config_error_code)?;
            }

            write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 7 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.error_message)?;
        } else {
            total_size += types::String.compute_size(&self.error_message)?;
        }
        if version >= 5 {
            total_size += types::Int32.compute_size(&self.num_partitions)?;
        }
        if version >= 5 {
            total_size += types::Int16.compute_size(&self.replication_factor)?;
        }
        if version >= 5 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.configs)?;
        }
        if version >= 5 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if self.topic_config_error_code != 0 {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
            if self.topic_config_error_code != 0 {
                let computed_size = types::Int16.compute_size(&self.topic_config_error_code)?;
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

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for CreatableTopicResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let topic_id = if version >= 7 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let error_code = types::Int16.decode(buf)?;
        let error_message = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let mut topic_config_error_code = 0;
        let num_partitions = if version >= 5 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let replication_factor = if version >= 5 {
            types::Int16.decode(buf)?
        } else {
            -1
        };
        let configs = if version >= 5 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Some(Default::default())
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 5 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        topic_config_error_code = types::Int16.decode(buf)?;
                    }
                    _ => {
                        let unknown_value = buf.try_get_bytes(size as usize)?;
                        unknown_tagged_fields.insert(tag as i32, unknown_value);
                    }
                }
            }
        }
        Ok(Self {
            name,
            topic_id,
            error_code,
            error_message,
            topic_config_error_code,
            num_partitions,
            replication_factor,
            configs,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreatableTopicResult {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            error_code: 0,
            error_message: Some(Default::default()),
            topic_config_error_code: 0,
            num_partitions: -1,
            replication_factor: -1,
            configs: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreatableTopicResult {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 2-7
    pub throttle_time_ms: i32,

    /// Results for each topic we tried to create.
    ///
    /// Supported API versions: 2-7
    pub topics: Vec<CreatableTopicResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreateTopicsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 2-7
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Results for each topic we tried to create.
    ///
    /// Supported API versions: 2-7
    pub fn with_topics(mut self, value: Vec<CreatableTopicResult>) -> Self {
        self.topics = value;
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
impl Encodable for CreateTopicsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        if version >= 5 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 5 {
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        if version >= 5 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 5 {
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
impl Decodable for CreateTopicsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let topics = if version >= 5 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 5 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreateTopicsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreateTopicsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for CreateTopicsResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 5 {
            1
        } else {
            0
        }
    }
}
