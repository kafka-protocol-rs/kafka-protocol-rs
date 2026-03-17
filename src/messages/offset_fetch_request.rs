//! OffsetFetchRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/OffsetFetchRequest.json).
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

/// Valid versions: 1-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    ///
    /// Supported API versions: 1-7
    pub group_id: super::GroupId,

    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    ///
    /// Supported API versions: 1-7
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,

    /// Each group we would like to fetch offsets for.
    ///
    /// Supported API versions: 8-9
    pub groups: Vec<OffsetFetchRequestGroup>,

    /// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
    ///
    /// Supported API versions: 7-9
    pub require_stable: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The group to fetch offsets for.
    ///
    /// Supported API versions: 1-7
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    ///
    /// Supported API versions: 1-7
    pub fn with_topics(mut self, value: Option<Vec<OffsetFetchRequestTopic>>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `groups` to the passed value.
    ///
    /// Each group we would like to fetch offsets for.
    ///
    /// Supported API versions: 8-9
    pub fn with_groups(mut self, value: Vec<OffsetFetchRequestGroup>) -> Self {
        self.groups = value;
        self
    }
    /// Sets `require_stable` to the passed value.
    ///
    /// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
    ///
    /// Supported API versions: 7-9
    pub fn with_require_stable(mut self, value: bool) -> Self {
        self.require_stable = value;
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
impl Encodable for OffsetFetchRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactString.encode(buf, &self.group_id)?;
            } else {
                types::String.encode(buf, &self.group_id)?;
            }
        } else {
            if !self.group_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.topics)?;
            }
        } else {
            if !self
                .topics
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.groups)?;
        } else {
            if !self.groups.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 7 {
            types::Boolean.encode(buf, &self.require_stable)?;
        } else {
            if self.require_stable {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
        if version <= 7 {
            if version >= 6 {
                total_size += types::CompactString.compute_size(&self.group_id)?;
            } else {
                total_size += types::String.compute_size(&self.group_id)?;
            }
        } else {
            if !self.group_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            if version >= 6 {
                total_size +=
                    types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
            } else {
                total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
            }
        } else {
            if !self
                .topics
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.groups)?;
        } else {
            if !self.groups.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 7 {
            total_size += types::Boolean.compute_size(&self.require_stable)?;
        } else {
            if self.require_stable {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
impl Decodable for OffsetFetchRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version <= 7 {
            if version >= 6 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let topics = if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
            }
        } else {
            Some(Default::default())
        };
        let groups = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let require_stable = if version >= 7 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            group_id,
            topics,
            groups,
            require_stable,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            topics: Some(Default::default()),
            groups: Default::default(),
            require_stable: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestGroup {
    /// The group ID.
    ///
    /// Supported API versions: 8-9
    pub group_id: super::GroupId,

    /// The member id.
    ///
    /// Supported API versions: 9
    pub member_id: Option<StrBytes>,

    /// The member epoch if using the new consumer protocol (KIP-848).
    ///
    /// Supported API versions: 9
    pub member_epoch: i32,

    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    ///
    /// Supported API versions: 8-9
    pub topics: Option<Vec<OffsetFetchRequestTopics>>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchRequestGroup {
    /// Sets `group_id` to the passed value.
    ///
    /// The group ID.
    ///
    /// Supported API versions: 8-9
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member id.
    ///
    /// Supported API versions: 9
    pub fn with_member_id(mut self, value: Option<StrBytes>) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `member_epoch` to the passed value.
    ///
    /// The member epoch if using the new consumer protocol (KIP-848).
    ///
    /// Supported API versions: 9
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    ///
    /// Supported API versions: 8-9
    pub fn with_topics(mut self, value: Option<Vec<OffsetFetchRequestTopics>>) -> Self {
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

#[cfg(feature = "client")]
impl Encodable for OffsetFetchRequestGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.group_id)?;
        } else {
            if !self.group_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
            types::CompactString.encode(buf, &self.member_id)?;
        }
        if version >= 9 {
            types::Int32.encode(buf, &self.member_epoch)?;
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            if !self
                .topics
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.group_id)?;
        } else {
            if !self.group_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
            total_size += types::CompactString.compute_size(&self.member_id)?;
        }
        if version >= 9 {
            total_size += types::Int32.compute_size(&self.member_epoch)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            if !self
                .topics
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
impl Decodable for OffsetFetchRequestGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let member_id = if version >= 9 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let member_epoch = if version >= 9 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let topics = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Some(Default::default())
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchRequestGroup {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            member_id: None,
            member_epoch: -1,
            topics: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchRequestGroup {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    ///
    /// Supported API versions: 1-7
    pub name: super::TopicName,

    /// The partition indexes we would like to fetch offsets for.
    ///
    /// Supported API versions: 1-7
    pub partition_indexes: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchRequestTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 1-7
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partition_indexes` to the passed value.
    ///
    /// The partition indexes we would like to fetch offsets for.
    ///
    /// Supported API versions: 1-7
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
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
impl Encodable for OffsetFetchRequestTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactString.encode(buf, &self.name)?;
            } else {
                types::String.encode(buf, &self.name)?;
            }
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Int32).encode(buf, &self.partition_indexes)?;
            } else {
                types::Array(types::Int32).encode(buf, &self.partition_indexes)?;
            }
        } else {
            if !self.partition_indexes.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
        if version <= 7 {
            if version >= 6 {
                total_size += types::CompactString.compute_size(&self.name)?;
            } else {
                total_size += types::String.compute_size(&self.name)?;
            }
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            if version >= 6 {
                total_size +=
                    types::CompactArray(types::Int32).compute_size(&self.partition_indexes)?;
            } else {
                total_size += types::Array(types::Int32).compute_size(&self.partition_indexes)?;
            }
        } else {
            if !self.partition_indexes.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
impl Decodable for OffsetFetchRequestTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let name = if version <= 7 {
            if version >= 6 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let partition_indexes = if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Int32).decode(buf)?
            } else {
                types::Array(types::Int32).decode(buf)?
            }
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
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
            partition_indexes,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchRequestTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partition_indexes: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchRequestTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchRequestTopics {
    /// The topic name.
    ///
    /// Supported API versions: 8-9
    pub name: super::TopicName,

    /// The topic ID.
    ///
    /// Supported API versions: none
    pub topic_id: Uuid,

    /// The partition indexes we would like to fetch offsets for.
    ///
    /// Supported API versions: 8-9
    pub partition_indexes: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchRequestTopics {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 8-9
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic ID.
    ///
    /// Supported API versions: none
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partition_indexes` to the passed value.
    ///
    /// The partition indexes we would like to fetch offsets for.
    ///
    /// Supported API versions: 8-9
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
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
impl Encodable for OffsetFetchRequestTopics {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.name)?;
        }
        if version >= 8 {
            types::CompactArray(types::Int32).encode(buf, &self.partition_indexes)?;
        } else {
            if !self.partition_indexes.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.name)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Int32).compute_size(&self.partition_indexes)?;
        } else {
            if !self.partition_indexes.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
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
impl Decodable for OffsetFetchRequestTopics {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let topic_id = Uuid::nil();
        let partition_indexes = if version >= 8 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
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
            partition_indexes,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchRequestTopics {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            partition_indexes: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchRequestTopics {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for OffsetFetchRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 6 {
            2
        } else {
            1
        }
    }
}
