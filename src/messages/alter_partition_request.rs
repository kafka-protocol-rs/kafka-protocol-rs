//! AlterPartitionRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/AlterPartitionRequest.json).
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

/// Valid versions: 2-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AlterPartitionRequest {
    /// The ID of the requesting broker.
    ///
    /// Supported API versions: 2-3
    pub broker_id: super::BrokerId,

    /// The epoch of the requesting broker.
    ///
    /// Supported API versions: 2-3
    pub broker_epoch: i64,

    /// The topics to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub topics: Vec<TopicData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AlterPartitionRequest {
    /// Sets `broker_id` to the passed value.
    ///
    /// The ID of the requesting broker.
    ///
    /// Supported API versions: 2-3
    pub fn with_broker_id(mut self, value: super::BrokerId) -> Self {
        self.broker_id = value;
        self
    }
    /// Sets `broker_epoch` to the passed value.
    ///
    /// The epoch of the requesting broker.
    ///
    /// Supported API versions: 2-3
    pub fn with_broker_epoch(mut self, value: i64) -> Self {
        self.broker_epoch = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub fn with_topics(mut self, value: Vec<TopicData>) -> Self {
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
impl Encodable for AlterPartitionRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.broker_id)?;
        types::Int64.encode(buf, &self.broker_epoch)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
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
        total_size += types::Int32.compute_size(&self.broker_id)?;
        total_size += types::Int64.compute_size(&self.broker_epoch)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
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
impl Decodable for AlterPartitionRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let broker_id = types::Int32.decode(buf)?;
        let broker_epoch = types::Int64.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for AlterPartitionRequest {
    fn default() -> Self {
        Self {
            broker_id: (0).into(),
            broker_epoch: -1,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AlterPartitionRequest {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerState {
    /// The ID of the broker.
    ///
    /// Supported API versions: 3
    pub broker_id: super::BrokerId,

    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    ///
    /// Supported API versions: 3
    pub broker_epoch: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl BrokerState {
    /// Sets `broker_id` to the passed value.
    ///
    /// The ID of the broker.
    ///
    /// Supported API versions: 3
    pub fn with_broker_id(mut self, value: super::BrokerId) -> Self {
        self.broker_id = value;
        self
    }
    /// Sets `broker_epoch` to the passed value.
    ///
    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    ///
    /// Supported API versions: 3
    pub fn with_broker_epoch(mut self, value: i64) -> Self {
        self.broker_epoch = value;
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
impl Encodable for BrokerState {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::Int32.encode(buf, &self.broker_id)?;
        } else {
            if self.broker_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int64.encode(buf, &self.broker_epoch)?;
        } else {
            if self.broker_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
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
        if version >= 3 {
            total_size += types::Int32.compute_size(&self.broker_id)?;
        } else {
            if self.broker_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int64.compute_size(&self.broker_epoch)?;
        } else {
            if self.broker_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
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
impl Decodable for BrokerState {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let broker_id = if version >= 3 {
            types::Int32.decode(buf)?
        } else {
            (0).into()
        };
        let broker_epoch = if version >= 3 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for BrokerState {
    fn default() -> Self {
        Self {
            broker_id: (0).into(),
            broker_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for BrokerState {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    ///
    /// Supported API versions: 2-3
    pub partition_index: i32,

    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 2-3
    pub leader_epoch: i32,

    /// The ISR for this partition. Deprecated since version 3.
    ///
    /// Supported API versions: 2
    pub new_isr: Vec<super::BrokerId>,

    /// The ISR for this partition.
    ///
    /// Supported API versions: 3
    pub new_isr_with_epochs: Vec<BrokerState>,

    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    ///
    /// Supported API versions: 2-3
    pub leader_recovery_state: i8,

    /// The expected epoch of the partition which is being updated.
    ///
    /// Supported API versions: 2-3
    pub partition_epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionData {
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 2-3
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 2-3
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    /// Sets `new_isr` to the passed value.
    ///
    /// The ISR for this partition. Deprecated since version 3.
    ///
    /// Supported API versions: 2
    pub fn with_new_isr(mut self, value: Vec<super::BrokerId>) -> Self {
        self.new_isr = value;
        self
    }
    /// Sets `new_isr_with_epochs` to the passed value.
    ///
    /// The ISR for this partition.
    ///
    /// Supported API versions: 3
    pub fn with_new_isr_with_epochs(mut self, value: Vec<BrokerState>) -> Self {
        self.new_isr_with_epochs = value;
        self
    }
    /// Sets `leader_recovery_state` to the passed value.
    ///
    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    ///
    /// Supported API versions: 2-3
    pub fn with_leader_recovery_state(mut self, value: i8) -> Self {
        self.leader_recovery_state = value;
        self
    }
    /// Sets `partition_epoch` to the passed value.
    ///
    /// The expected epoch of the partition which is being updated.
    ///
    /// Supported API versions: 2-3
    pub fn with_partition_epoch(mut self, value: i32) -> Self {
        self.partition_epoch = value;
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
impl Encodable for PartitionData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int32.encode(buf, &self.leader_epoch)?;
        if version == 2 {
            types::CompactArray(types::Int32).encode(buf, &self.new_isr)?;
        } else {
            if !self.new_isr.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::CompactArray(types::Struct { version })
                .encode(buf, &self.new_isr_with_epochs)?;
        } else {
            if !self.new_isr_with_epochs.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        types::Int8.encode(buf, &self.leader_recovery_state)?;
        types::Int32.encode(buf, &self.partition_epoch)?;
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
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int32.compute_size(&self.leader_epoch)?;
        if version == 2 {
            total_size += types::CompactArray(types::Int32).compute_size(&self.new_isr)?;
        } else {
            if !self.new_isr.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.new_isr_with_epochs)?;
        } else {
            if !self.new_isr_with_epochs.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        total_size += types::Int8.compute_size(&self.leader_recovery_state)?;
        total_size += types::Int32.compute_size(&self.partition_epoch)?;
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
impl Decodable for PartitionData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let partition_index = types::Int32.decode(buf)?;
        let leader_epoch = types::Int32.decode(buf)?;
        let new_isr = if version == 2 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            Default::default()
        };
        let new_isr_with_epochs = if version >= 3 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let leader_recovery_state = types::Int8.decode(buf)?;
        let partition_epoch = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            partition_index,
            leader_epoch,
            new_isr,
            new_isr_with_epochs,
            leader_recovery_state,
            partition_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self {
            partition_index: 0,
            leader_epoch: 0,
            new_isr: Default::default(),
            new_isr_with_epochs: Default::default(),
            leader_recovery_state: 0,
            partition_epoch: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionData {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 2-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicData {
    /// The ID of the topic to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub topic_id: Uuid,

    /// The partitions to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub partitions: Vec<PartitionData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicData {
    /// Sets `topic_id` to the passed value.
    ///
    /// The ID of the topic to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions to alter ISRs for.
    ///
    /// Supported API versions: 2-3
    pub fn with_partitions(mut self, value: Vec<PartitionData>) -> Self {
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
impl Encodable for TopicData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.topic_id)?;
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
        total_size += types::Uuid.compute_size(&self.topic_id)?;
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
impl Decodable for TopicData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 2 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = types::Uuid.decode(buf)?;
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
            topic_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicData {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicData {
    const VERSIONS: VersionRange = VersionRange { min: 2, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for AlterPartitionRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
