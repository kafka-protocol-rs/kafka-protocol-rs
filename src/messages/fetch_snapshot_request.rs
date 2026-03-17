//! FetchSnapshotRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchSnapshotRequest.json).
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

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchSnapshotRequest {
    /// The clusterId if known, this is used to validate metadata fetches prior to broker registration.
    ///
    /// Supported API versions: 0-1
    pub cluster_id: Option<StrBytes>,

    /// The broker ID of the follower.
    ///
    /// Supported API versions: 0-1
    pub replica_id: super::BrokerId,

    /// The maximum bytes to fetch from all of the snapshots.
    ///
    /// Supported API versions: 0-1
    pub max_bytes: i32,

    /// The topics to fetch.
    ///
    /// Supported API versions: 0-1
    pub topics: Vec<TopicSnapshot>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchSnapshotRequest {
    /// Sets `cluster_id` to the passed value.
    ///
    /// The clusterId if known, this is used to validate metadata fetches prior to broker registration.
    ///
    /// Supported API versions: 0-1
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `replica_id` to the passed value.
    ///
    /// The broker ID of the follower.
    ///
    /// Supported API versions: 0-1
    pub fn with_replica_id(mut self, value: super::BrokerId) -> Self {
        self.replica_id = value;
        self
    }
    /// Sets `max_bytes` to the passed value.
    ///
    /// The maximum bytes to fetch from all of the snapshots.
    ///
    /// Supported API versions: 0-1
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to fetch.
    ///
    /// Supported API versions: 0-1
    pub fn with_topics(mut self, value: Vec<TopicSnapshot>) -> Self {
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
impl Encodable for FetchSnapshotRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.replica_id)?;
        types::Int32.encode(buf, &self.max_bytes)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if !self.cluster_id.is_none() {
            num_tagged_fields += 1;
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
        if !self.cluster_id.is_none() {
            let computed_size = types::CompactString.compute_size(&self.cluster_id)?;
            if computed_size > std::u32::MAX as usize {
                bail!(
                    "Tagged field is too large to encode ({} bytes)",
                    computed_size
                );
            }
            types::UnsignedVarInt.encode(buf, 0)?;
            types::UnsignedVarInt.encode(buf, computed_size as u32)?;
            types::CompactString.encode(buf, &self.cluster_id)?;
        }

        write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.replica_id)?;
        total_size += types::Int32.compute_size(&self.max_bytes)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if !self.cluster_id.is_none() {
            num_tagged_fields += 1;
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
        if !self.cluster_id.is_none() {
            let computed_size = types::CompactString.compute_size(&self.cluster_id)?;
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
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for FetchSnapshotRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let mut cluster_id = None;
        let replica_id = types::Int32.decode(buf)?;
        let max_bytes = types::Int32.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            match tag {
                0 => {
                    cluster_id = types::CompactString.decode(buf)?;
                }
                _ => {
                    let unknown_value = buf.try_get_bytes(size as usize)?;
                    unknown_tagged_fields.insert(tag as i32, unknown_value);
                }
            }
        }
        Ok(Self {
            cluster_id,
            replica_id,
            max_bytes,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchSnapshotRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            replica_id: (-1).into(),
            max_bytes: 0x7fffffff,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchSnapshotRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    ///
    /// Supported API versions: 0-1
    pub partition: i32,

    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    ///
    /// Supported API versions: 0-1
    pub current_leader_epoch: i32,

    /// The snapshot endOffset and epoch to fetch.
    ///
    /// Supported API versions: 0-1
    pub snapshot_id: SnapshotId,

    /// The byte position within the snapshot to start fetching from.
    ///
    /// Supported API versions: 0-1
    pub position: i64,

    /// The directory id of the follower fetching.
    ///
    /// Supported API versions: 1
    pub replica_directory_id: Uuid,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionSnapshot {
    /// Sets `partition` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0-1
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    /// Sets `current_leader_epoch` to the passed value.
    ///
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    ///
    /// Supported API versions: 0-1
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    /// Sets `snapshot_id` to the passed value.
    ///
    /// The snapshot endOffset and epoch to fetch.
    ///
    /// Supported API versions: 0-1
    pub fn with_snapshot_id(mut self, value: SnapshotId) -> Self {
        self.snapshot_id = value;
        self
    }
    /// Sets `position` to the passed value.
    ///
    /// The byte position within the snapshot to start fetching from.
    ///
    /// Supported API versions: 0-1
    pub fn with_position(mut self, value: i64) -> Self {
        self.position = value;
        self
    }
    /// Sets `replica_directory_id` to the passed value.
    ///
    /// The directory id of the follower fetching.
    ///
    /// Supported API versions: 1
    pub fn with_replica_directory_id(mut self, value: Uuid) -> Self {
        self.replica_directory_id = value;
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
impl Encodable for PartitionSnapshot {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition)?;
        types::Int32.encode(buf, &self.current_leader_epoch)?;
        types::Struct { version }.encode(buf, &self.snapshot_id)?;
        types::Int64.encode(buf, &self.position)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if &self.replica_directory_id != &Uuid::nil() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
        if version >= 1 {
            if &self.replica_directory_id != &Uuid::nil() {
                let computed_size = types::Uuid.compute_size(&self.replica_directory_id)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Uuid.encode(buf, &self.replica_directory_id)?;
            }
        }
        write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition)?;
        total_size += types::Int32.compute_size(&self.current_leader_epoch)?;
        total_size += types::Struct { version }.compute_size(&self.snapshot_id)?;
        total_size += types::Int64.compute_size(&self.position)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if &self.replica_directory_id != &Uuid::nil() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
        if version >= 1 {
            if &self.replica_directory_id != &Uuid::nil() {
                let computed_size = types::Uuid.compute_size(&self.replica_directory_id)?;
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
        }
        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for PartitionSnapshot {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let partition = types::Int32.decode(buf)?;
        let current_leader_epoch = types::Int32.decode(buf)?;
        let snapshot_id = types::Struct { version }.decode(buf)?;
        let position = types::Int64.decode(buf)?;
        let mut replica_directory_id = Uuid::nil();
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            match tag {
                0 => {
                    if version >= 1 {
                        replica_directory_id = types::Uuid.decode(buf)?;
                    } else {
                        bail!("Tag {} is not valid for version {}", tag, version);
                    }
                }
                _ => {
                    let unknown_value = buf.try_get_bytes(size as usize)?;
                    unknown_tagged_fields.insert(tag as i32, unknown_value);
                }
            }
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            snapshot_id,
            position,
            replica_directory_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionSnapshot {
    fn default() -> Self {
        Self {
            partition: 0,
            current_leader_epoch: 0,
            snapshot_id: Default::default(),
            position: 0,
            replica_directory_id: Uuid::nil(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionSnapshot {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the snapshot.
    ///
    /// Supported API versions: 0-1
    pub end_offset: i64,

    /// The epoch of the snapshot.
    ///
    /// Supported API versions: 0-1
    pub epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SnapshotId {
    /// Sets `end_offset` to the passed value.
    ///
    /// The end offset of the snapshot.
    ///
    /// Supported API versions: 0-1
    pub fn with_end_offset(mut self, value: i64) -> Self {
        self.end_offset = value;
        self
    }
    /// Sets `epoch` to the passed value.
    ///
    /// The epoch of the snapshot.
    ///
    /// Supported API versions: 0-1
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
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
impl Encodable for SnapshotId {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.end_offset)?;
        types::Int32.encode(buf, &self.epoch)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int64.compute_size(&self.end_offset)?;
        total_size += types::Int32.compute_size(&self.epoch)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for SnapshotId {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let end_offset = types::Int64.decode(buf)?;
        let epoch = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            end_offset,
            epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self {
            end_offset: 0,
            epoch: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SnapshotId {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicSnapshot {
    /// The name of the topic to fetch.
    ///
    /// Supported API versions: 0-1
    pub name: super::TopicName,

    /// The partitions to fetch.
    ///
    /// Supported API versions: 0-1
    pub partitions: Vec<PartitionSnapshot>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicSnapshot {
    /// Sets `name` to the passed value.
    ///
    /// The name of the topic to fetch.
    ///
    /// Supported API versions: 0-1
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions to fetch.
    ///
    /// Supported API versions: 0-1
    pub fn with_partitions(mut self, value: Vec<PartitionSnapshot>) -> Self {
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
impl Encodable for TopicSnapshot {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopicSnapshot {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            name,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicSnapshot {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicSnapshot {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for FetchSnapshotRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
