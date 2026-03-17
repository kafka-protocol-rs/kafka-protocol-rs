//! OffsetFetchResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/OffsetFetchResponse.json).
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

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-10
    pub throttle_time_ms: i32,

    /// The responses per topic.
    ///
    /// Supported API versions: 1-7
    pub topics: Vec<OffsetFetchResponseTopic>,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 2-7
    pub error_code: i16,

    /// The responses per group id.
    ///
    /// Supported API versions: 8-10
    pub groups: Vec<OffsetFetchResponseGroup>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-10
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The responses per topic.
    ///
    /// Supported API versions: 1-7
    pub fn with_topics(mut self, value: Vec<OffsetFetchResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 2-7
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `groups` to the passed value.
    ///
    /// The responses per group id.
    ///
    /// Supported API versions: 8-10
    pub fn with_groups(mut self, value: Vec<OffsetFetchResponseGroup>) -> Self {
        self.groups = value;
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
impl Encodable for OffsetFetchResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.topics)?;
            }
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 && version <= 7 {
            types::Int16.encode(buf, &self.error_code)?;
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.groups)?;
        } else {
            if !self.groups.is_empty() {
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
        if version >= 3 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        if version <= 7 {
            if version >= 6 {
                total_size +=
                    types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
            } else {
                total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
            }
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 && version <= 7 {
            total_size += types::Int16.compute_size(&self.error_code)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.groups)?;
        } else {
            if !self.groups.is_empty() {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = if version >= 3 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let topics = if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
            }
        } else {
            Default::default()
        };
        let error_code = if version >= 2 && version <= 7 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let groups = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
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
            throttle_time_ms,
            topics,
            error_code,
            groups,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            topics: Default::default(),
            error_code: 0,
            groups: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseGroup {
    /// The group ID.
    ///
    /// Supported API versions: 8-10
    pub group_id: super::GroupId,

    /// The responses per topic.
    ///
    /// Supported API versions: 8-10
    pub topics: Vec<OffsetFetchResponseTopics>,

    /// The group-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 8-10
    pub error_code: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponseGroup {
    /// Sets `group_id` to the passed value.
    ///
    /// The group ID.
    ///
    /// Supported API versions: 8-10
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The responses per topic.
    ///
    /// Supported API versions: 8-10
    pub fn with_topics(mut self, value: Vec<OffsetFetchResponseTopics>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The group-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 8-10
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
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
impl Encodable for OffsetFetchResponseGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.group_id)?;
        } else {
            if !self.group_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::Int16.encode(buf, &self.error_code)?;
        } else {
            if self.error_code != 0 {
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
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size += types::Int16.compute_size(&self.error_code)?;
        } else {
            if self.error_code != 0 {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponseGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let topics = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let error_code = if version >= 8 {
            types::Int16.decode(buf)?
        } else {
            0
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
            error_code,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponseGroup {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            topics: Default::default(),
            error_code: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponseGroup {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponsePartition {
    /// The partition index.
    ///
    /// Supported API versions: 1-7
    pub partition_index: i32,

    /// The committed message offset.
    ///
    /// Supported API versions: 1-7
    pub committed_offset: i64,

    /// The leader epoch.
    ///
    /// Supported API versions: 5-7
    pub committed_leader_epoch: i32,

    /// The partition metadata.
    ///
    /// Supported API versions: 1-7
    pub metadata: Option<StrBytes>,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-7
    pub error_code: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponsePartition {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 1-7
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `committed_offset` to the passed value.
    ///
    /// The committed message offset.
    ///
    /// Supported API versions: 1-7
    pub fn with_committed_offset(mut self, value: i64) -> Self {
        self.committed_offset = value;
        self
    }
    /// Sets `committed_leader_epoch` to the passed value.
    ///
    /// The leader epoch.
    ///
    /// Supported API versions: 5-7
    pub fn with_committed_leader_epoch(mut self, value: i32) -> Self {
        self.committed_leader_epoch = value;
        self
    }
    /// Sets `metadata` to the passed value.
    ///
    /// The partition metadata.
    ///
    /// Supported API versions: 1-7
    pub fn with_metadata(mut self, value: Option<StrBytes>) -> Self {
        self.metadata = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-7
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
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
impl Encodable for OffsetFetchResponsePartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        if version <= 7 {
            types::Int32.encode(buf, &self.partition_index)?;
        } else {
            if self.partition_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            types::Int64.encode(buf, &self.committed_offset)?;
        } else {
            if self.committed_offset != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 && version <= 7 {
            types::Int32.encode(buf, &self.committed_leader_epoch)?;
        }
        if version <= 7 {
            if version >= 6 {
                types::CompactString.encode(buf, &self.metadata)?;
            } else {
                types::String.encode(buf, &self.metadata)?;
            }
        } else {
            if !self
                .metadata
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            types::Int16.encode(buf, &self.error_code)?;
        } else {
            if self.error_code != 0 {
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
            total_size += types::Int32.compute_size(&self.partition_index)?;
        } else {
            if self.partition_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            total_size += types::Int64.compute_size(&self.committed_offset)?;
        } else {
            if self.committed_offset != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 && version <= 7 {
            total_size += types::Int32.compute_size(&self.committed_leader_epoch)?;
        }
        if version <= 7 {
            if version >= 6 {
                total_size += types::CompactString.compute_size(&self.metadata)?;
            } else {
                total_size += types::String.compute_size(&self.metadata)?;
            }
        } else {
            if !self
                .metadata
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 7 {
            total_size += types::Int16.compute_size(&self.error_code)?;
        } else {
            if self.error_code != 0 {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponsePartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = if version <= 7 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let committed_offset = if version <= 7 {
            types::Int64.decode(buf)?
        } else {
            0
        };
        let committed_leader_epoch = if version >= 5 && version <= 7 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let metadata = if version <= 7 {
            if version >= 6 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Some(Default::default())
        };
        let error_code = if version <= 7 {
            types::Int16.decode(buf)?
        } else {
            0
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
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponsePartition {
    fn default() -> Self {
        Self {
            partition_index: 0,
            committed_offset: 0,
            committed_leader_epoch: -1,
            metadata: Some(Default::default()),
            error_code: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponsePartition {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponsePartitions {
    /// The partition index.
    ///
    /// Supported API versions: 8-10
    pub partition_index: i32,

    /// The committed message offset.
    ///
    /// Supported API versions: 8-10
    pub committed_offset: i64,

    /// The leader epoch.
    ///
    /// Supported API versions: 8-10
    pub committed_leader_epoch: i32,

    /// The partition metadata.
    ///
    /// Supported API versions: 8-10
    pub metadata: Option<StrBytes>,

    /// The partition-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 8-10
    pub error_code: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponsePartitions {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 8-10
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `committed_offset` to the passed value.
    ///
    /// The committed message offset.
    ///
    /// Supported API versions: 8-10
    pub fn with_committed_offset(mut self, value: i64) -> Self {
        self.committed_offset = value;
        self
    }
    /// Sets `committed_leader_epoch` to the passed value.
    ///
    /// The leader epoch.
    ///
    /// Supported API versions: 8-10
    pub fn with_committed_leader_epoch(mut self, value: i32) -> Self {
        self.committed_leader_epoch = value;
        self
    }
    /// Sets `metadata` to the passed value.
    ///
    /// The partition metadata.
    ///
    /// Supported API versions: 8-10
    pub fn with_metadata(mut self, value: Option<StrBytes>) -> Self {
        self.metadata = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The partition-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 8-10
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
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
impl Encodable for OffsetFetchResponsePartitions {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::Int32.encode(buf, &self.partition_index)?;
        } else {
            if self.partition_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::Int64.encode(buf, &self.committed_offset)?;
        } else {
            if self.committed_offset != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::Int32.encode(buf, &self.committed_leader_epoch)?;
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.metadata)?;
        } else {
            if !self
                .metadata
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::Int16.encode(buf, &self.error_code)?;
        } else {
            if self.error_code != 0 {
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
            total_size += types::Int32.compute_size(&self.partition_index)?;
        } else {
            if self.partition_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size += types::Int64.compute_size(&self.committed_offset)?;
        } else {
            if self.committed_offset != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size += types::Int32.compute_size(&self.committed_leader_epoch)?;
        }
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.metadata)?;
        } else {
            if !self
                .metadata
                .as_ref()
                .map(|x| x.is_empty())
                .unwrap_or_default()
            {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size += types::Int16.compute_size(&self.error_code)?;
        } else {
            if self.error_code != 0 {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponsePartitions {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = if version >= 8 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let committed_offset = if version >= 8 {
            types::Int64.decode(buf)?
        } else {
            0
        };
        let committed_leader_epoch = if version >= 8 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let metadata = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            Some(Default::default())
        };
        let error_code = if version >= 8 {
            types::Int16.decode(buf)?
        } else {
            0
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
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponsePartitions {
    fn default() -> Self {
        Self {
            partition_index: 0,
            committed_offset: 0,
            committed_leader_epoch: -1,
            metadata: Some(Default::default()),
            error_code: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponsePartitions {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    ///
    /// Supported API versions: 1-7
    pub name: super::TopicName,

    /// The responses per partition.
    ///
    /// Supported API versions: 1-7
    pub partitions: Vec<OffsetFetchResponsePartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponseTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 1-7
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The responses per partition.
    ///
    /// Supported API versions: 1-7
    pub fn with_partitions(mut self, value: Vec<OffsetFetchResponsePartition>) -> Self {
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
impl Encodable for OffsetFetchResponseTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
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
                types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
            }
        } else {
            if !self.partitions.is_empty() {
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
                total_size += types::CompactArray(types::Struct { version })
                    .compute_size(&self.partitions)?;
            } else {
                total_size +=
                    types::Array(types::Struct { version }).compute_size(&self.partitions)?;
            }
        } else {
            if !self.partitions.is_empty() {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponseTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
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
        let partitions = if version <= 7 {
            if version >= 6 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
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
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponseTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponseTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-10
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetFetchResponseTopics {
    /// The topic name.
    ///
    /// Supported API versions: 8-9
    pub name: super::TopicName,

    /// The topic ID.
    ///
    /// Supported API versions: 10
    pub topic_id: Uuid,

    /// The responses per partition.
    ///
    /// Supported API versions: 8-10
    pub partitions: Vec<OffsetFetchResponsePartitions>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl OffsetFetchResponseTopics {
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
    /// Supported API versions: 10
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The responses per partition.
    ///
    /// Supported API versions: 8-10
    pub fn with_partitions(mut self, value: Vec<OffsetFetchResponsePartitions>) -> Self {
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
impl Encodable for OffsetFetchResponseTopics {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 && version <= 9 {
            types::CompactString.encode(buf, &self.name)?;
        }
        if version >= 10 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 8 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            if !self.partitions.is_empty() {
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
        if version >= 8 && version <= 9 {
            total_size += types::CompactString.compute_size(&self.name)?;
        }
        if version >= 10 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 8 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            if !self.partitions.is_empty() {
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

#[cfg(feature = "client")]
impl Decodable for OffsetFetchResponseTopics {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 10 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 8 && version <= 9 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let topic_id = if version >= 10 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let partitions = if version >= 8 {
            types::CompactArray(types::Struct { version }).decode(buf)?
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
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for OffsetFetchResponseTopics {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for OffsetFetchResponseTopics {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 10 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for OffsetFetchResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 6 {
            1
        } else {
            0
        }
    }
}
