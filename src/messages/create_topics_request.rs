//! CreateTopicsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/CreateTopicsRequest.json).
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
pub struct CreatableReplicaAssignment {
    /// The partition index.
    ///
    /// Supported API versions: 2-7
    pub partition_index: i32,

    /// The brokers to place the partition on.
    ///
    /// Supported API versions: 2-7
    pub broker_ids: Vec<super::BrokerId>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreatableReplicaAssignment {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 2-7
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `broker_ids` to the passed value.
    ///
    /// The brokers to place the partition on.
    ///
    /// Supported API versions: 2-7
    pub fn with_broker_ids(mut self, value: Vec<super::BrokerId>) -> Self {
        self.broker_ids = value;
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
impl Encodable for CreatableReplicaAssignment {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        if version >= 5 {
            types::CompactArray(types::Int32).encode(buf, &self.broker_ids)?;
        } else {
            types::Array(types::Int32).encode(buf, &self.broker_ids)?;
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
        total_size += types::Int32.compute_size(&self.partition_index)?;
        if version >= 5 {
            total_size += types::CompactArray(types::Int32).compute_size(&self.broker_ids)?;
        } else {
            total_size += types::Array(types::Int32).compute_size(&self.broker_ids)?;
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

#[cfg(feature = "broker")]
impl Decodable for CreatableReplicaAssignment {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let broker_ids = if version >= 5 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            types::Array(types::Int32).decode(buf)?
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
            partition_index,
            broker_ids,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreatableReplicaAssignment {
    fn default() -> Self {
        Self {
            partition_index: 0,
            broker_ids: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreatableReplicaAssignment {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopic {
    /// The topic name.
    ///
    /// Supported API versions: 2-7
    pub name: super::TopicName,

    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    ///
    /// Supported API versions: 2-7
    pub num_partitions: i32,

    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    ///
    /// Supported API versions: 2-7
    pub replication_factor: i16,

    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    ///
    /// Supported API versions: 2-7
    pub assignments: Vec<CreatableReplicaAssignment>,

    /// The custom topic configurations to set.
    ///
    /// Supported API versions: 2-7
    pub configs: Vec<CreatableTopicConfig>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreatableTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 2-7
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `num_partitions` to the passed value.
    ///
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    ///
    /// Supported API versions: 2-7
    pub fn with_num_partitions(mut self, value: i32) -> Self {
        self.num_partitions = value;
        self
    }
    /// Sets `replication_factor` to the passed value.
    ///
    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    ///
    /// Supported API versions: 2-7
    pub fn with_replication_factor(mut self, value: i16) -> Self {
        self.replication_factor = value;
        self
    }
    /// Sets `assignments` to the passed value.
    ///
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    ///
    /// Supported API versions: 2-7
    pub fn with_assignments(mut self, value: Vec<CreatableReplicaAssignment>) -> Self {
        self.assignments = value;
        self
    }
    /// Sets `configs` to the passed value.
    ///
    /// The custom topic configurations to set.
    ///
    /// Supported API versions: 2-7
    pub fn with_configs(mut self, value: Vec<CreatableTopicConfig>) -> Self {
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

#[cfg(feature = "client")]
impl Encodable for CreatableTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        types::Int32.encode(buf, &self.num_partitions)?;
        types::Int16.encode(buf, &self.replication_factor)?;
        if version >= 5 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.assignments)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.assignments)?;
        }
        if version >= 5 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.configs)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.configs)?;
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
            total_size += types::String.compute_size(&self.name)?;
        }
        total_size += types::Int32.compute_size(&self.num_partitions)?;
        total_size += types::Int16.compute_size(&self.replication_factor)?;
        if version >= 5 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.assignments)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.assignments)?;
        }
        if version >= 5 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.configs)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.configs)?;
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

#[cfg(feature = "broker")]
impl Decodable for CreatableTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let num_partitions = types::Int32.decode(buf)?;
        let replication_factor = types::Int16.decode(buf)?;
        let assignments = if version >= 5 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let configs = if version >= 5 {
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
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreatableTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            num_partitions: 0,
            replication_factor: 0,
            assignments: Default::default(),
            configs: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreatableTopic {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreatableTopicConfig {
    /// The configuration name.
    ///
    /// Supported API versions: 2-7
    pub name: StrBytes,

    /// The configuration value.
    ///
    /// Supported API versions: 2-7
    pub value: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreatableTopicConfig {
    /// Sets `name` to the passed value.
    ///
    /// The configuration name.
    ///
    /// Supported API versions: 2-7
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `value` to the passed value.
    ///
    /// The configuration value.
    ///
    /// Supported API versions: 2-7
    pub fn with_value(mut self, value: Option<StrBytes>) -> Self {
        self.value = value;
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
impl Encodable for CreatableTopicConfig {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.value)?;
        } else {
            types::String.encode(buf, &self.value)?;
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
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.value)?;
        } else {
            total_size += types::String.compute_size(&self.value)?;
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

#[cfg(feature = "broker")]
impl Decodable for CreatableTopicConfig {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let value = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
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
            unknown_tagged_fields,
        })
    }
}

impl Default for CreatableTopicConfig {
    fn default() -> Self {
        Self {
            name: Default::default(),
            value: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreatableTopicConfig {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-7
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    ///
    /// Supported API versions: 2-7
    pub topics: Vec<CreatableTopic>,

    /// How long to wait in milliseconds before timing out the request.
    ///
    /// Supported API versions: 2-7
    pub timeout_ms: i32,

    /// If true, check that the topics can be created as specified, but don't create anything.
    ///
    /// Supported API versions: 2-7
    pub validate_only: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreateTopicsRequest {
    /// Sets `topics` to the passed value.
    ///
    /// The topics to create.
    ///
    /// Supported API versions: 2-7
    pub fn with_topics(mut self, value: Vec<CreatableTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `timeout_ms` to the passed value.
    ///
    /// How long to wait in milliseconds before timing out the request.
    ///
    /// Supported API versions: 2-7
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    /// Sets `validate_only` to the passed value.
    ///
    /// If true, check that the topics can be created as specified, but don't create anything.
    ///
    /// Supported API versions: 2-7
    pub fn with_validate_only(mut self, value: bool) -> Self {
        self.validate_only = value;
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
impl Encodable for CreateTopicsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        types::Int32.encode(buf, &self.timeout_ms)?;
        types::Boolean.encode(buf, &self.validate_only)?;
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
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        total_size += types::Int32.compute_size(&self.timeout_ms)?;
        total_size += types::Boolean.compute_size(&self.validate_only)?;
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

#[cfg(feature = "broker")]
impl Decodable for CreateTopicsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 7 {
            bail!("specified version not supported by this message type");
        }
        let topics = if version >= 5 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let timeout_ms = types::Int32.decode(buf)?;
        let validate_only = types::Boolean.decode(buf)?;
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
            topics,
            timeout_ms,
            validate_only,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreateTopicsRequest {
    fn default() -> Self {
        Self {
            topics: Default::default(),
            timeout_ms: 60000,
            validate_only: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreateTopicsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 7 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for CreateTopicsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 5 {
            2
        } else {
            1
        }
    }
}
