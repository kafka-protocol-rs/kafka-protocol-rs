//! OffsetCommitRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/OffsetCommitRequest.json).
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

/// Valid versions: 2-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    ///
    /// Supported API versions: 2-9
    pub group_id: super::GroupId,

    /// The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
    ///
    /// Supported API versions: 2-9
    pub generation_id_or_member_epoch: i32,

    /// The member ID assigned by the group coordinator.
    ///
    /// Supported API versions: 2-9
    pub member_id: StrBytes,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Supported API versions: 7-9
    pub group_instance_id: Option<StrBytes>,

    /// The time period in ms to retain the offset.
    ///
    /// Supported API versions: 2-4
    pub retention_time_ms: i64,

    /// The topics to commit offsets for.
    ///
    /// Supported API versions: 2-9
    pub topics: Vec<OffsetCommitRequestTopic>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetCommitRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The unique group identifier.
    ///
    /// Supported API versions: 2-9
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `generation_id_or_member_epoch` to the passed value.
    ///
    /// The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
    ///
    /// Supported API versions: 2-9
    pub fn with_generation_id_or_member_epoch(mut self, value: i32) -> Self {
        self.generation_id_or_member_epoch = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID assigned by the group coordinator.
    ///
    /// Supported API versions: 2-9
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `group_instance_id` to the passed value.
    ///
    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Supported API versions: 7-9
    pub fn with_group_instance_id(mut self, value: Option<StrBytes>) -> Self {
        self.group_instance_id = value;
        self
    }
    /// Sets `retention_time_ms` to the passed value.
    ///
    /// The time period in ms to retain the offset.
    ///
    /// Supported API versions: 2-4
    pub fn with_retention_time_ms(mut self, value: i64) -> Self {
        self.retention_time_ms = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to commit offsets for.
    ///
    /// Supported API versions: 2-9
    pub fn with_topics(mut self, value: Vec<OffsetCommitRequestTopic>) -> Self {
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
impl Encodable for OffsetCommitRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.group_id)?;
        } else {
            types::String.encode(buf, &self.group_id)?;
        }
        types::Int32.encode(buf, &self.generation_id_or_member_epoch)?;
        if version >= 8 {
            types::CompactString.encode(buf, &self.member_id)?;
        } else {
            types::String.encode(buf, &self.member_id)?;
        }
        if version >= 7 {
            if version >= 8 {
                types::CompactString.encode(buf, &self.group_instance_id)?;
            } else {
                types::String.encode(buf, &self.group_instance_id)?;
            }
        } else {
            if !self.group_instance_id.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 4 {
            types::Int64.encode(buf, &self.retention_time_ms)?;
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 8 {
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
            total_size += types::String.compute_size(&self.group_id)?;
        }
        total_size += types::Int32.compute_size(&self.generation_id_or_member_epoch)?;
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.member_id)?;
        } else {
            total_size += types::String.compute_size(&self.member_id)?;
        }
        if version >= 7 {
            if version >= 8 {
                total_size += types::CompactString.compute_size(&self.group_instance_id)?;
            } else {
                total_size += types::String.compute_size(&self.group_instance_id)?;
            }
        } else {
            if !self.group_instance_id.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 4 {
            total_size += types::Int64.compute_size(&self.retention_time_ms)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 8 {
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
impl Decodable for OffsetCommitRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let generation_id_or_member_epoch = types::Int32.decode(buf)?;
        let member_id = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let group_instance_id = if version >= 7 {
            if version >= 8 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            None
        };
        let retention_time_ms = if version <= 4 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let topics = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 8 {
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
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetCommitRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            generation_id_or_member_epoch: -1,
            member_id: Default::default(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetCommitRequest {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetCommitRequestPartition {
    /// The partition index.
    ///
    /// Supported API versions: 2-9
    pub partition_index: i32,

    /// The message offset to be committed.
    ///
    /// Supported API versions: 2-9
    pub committed_offset: i64,

    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 6-9
    pub committed_leader_epoch: i32,

    /// Any associated metadata the client wants to keep.
    ///
    /// Supported API versions: 2-9
    pub committed_metadata: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetCommitRequestPartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 2-9
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `committed_offset` to the passed value.
    ///
    /// The message offset to be committed.
    ///
    /// Supported API versions: 2-9
    pub fn with_committed_offset(mut self, value: i64) -> Self {
        self.committed_offset = value;
        self
    }
    /// Sets `committed_leader_epoch` to the passed value.
    ///
    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 6-9
    pub fn with_committed_leader_epoch(mut self, value: i32) -> Self {
        self.committed_leader_epoch = value;
        self
    }
    /// Sets `committed_metadata` to the passed value.
    ///
    /// Any associated metadata the client wants to keep.
    ///
    /// Supported API versions: 2-9
    pub fn with_committed_metadata(mut self, value: Option<StrBytes>) -> Self {
        self.committed_metadata = value;
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
impl Encodable for OffsetCommitRequestPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int64.encode(buf, &self.committed_offset)?;
        if version >= 6 {
            types::Int32.encode(buf, &self.committed_leader_epoch)?;
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.committed_metadata)?;
        } else {
            types::String.encode(buf, &self.committed_metadata)?;
        }
        if version >= 8 {
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
        total_size += types::Int64.compute_size(&self.committed_offset)?;
        if version >= 6 {
            total_size += types::Int32.compute_size(&self.committed_leader_epoch)?;
        }
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.committed_metadata)?;
        } else {
            total_size += types::String.compute_size(&self.committed_metadata)?;
        }
        if version >= 8 {
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
impl Decodable for OffsetCommitRequestPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let committed_offset = types::Int64.decode(buf)?;
        let committed_leader_epoch = if version >= 6 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let committed_metadata = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 8 {
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
            committed_offset,
            committed_leader_epoch,
            committed_metadata,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetCommitRequestPartition {
    fn default() -> Self {
        Self {
            partition_index: 0,
            committed_offset: 0,
            committed_leader_epoch: -1,
            committed_metadata: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetCommitRequestPartition {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    ///
    /// Supported API versions: 2-9
    pub name: super::TopicName,

    /// The topic ID.
    ///
    /// Supported API versions: none
    pub topic_id: Uuid,

    /// Each partition to commit offsets for.
    ///
    /// Supported API versions: 2-9
    pub partitions: Vec<OffsetCommitRequestPartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetCommitRequestTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 2-9
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
    /// Sets `partitions` to the passed value.
    ///
    /// Each partition to commit offsets for.
    ///
    /// Supported API versions: 2-9
    pub fn with_partitions(mut self, value: Vec<OffsetCommitRequestPartition>) -> Self {
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
impl Encodable for OffsetCommitRequestTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
        }
        if version >= 8 {
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
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;
        }
        if version >= 8 {
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
impl Decodable for OffsetCommitRequestTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let topic_id = Uuid::nil();
        let partitions = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 8 {
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
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetCommitRequestTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetCommitRequestTopic {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for OffsetCommitRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 8 {
            2
        } else {
            1
        }
    }
}
