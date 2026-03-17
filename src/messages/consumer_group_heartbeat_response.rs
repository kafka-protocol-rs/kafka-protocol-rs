//! ConsumerGroupHeartbeatResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerGroupHeartbeatResponse.json).
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
pub struct Assignment {
    /// The partitions assigned to the member that can be used immediately.
    ///
    /// Supported API versions: 0-1
    pub topic_partitions: Vec<TopicPartitions>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Assignment {
    /// Sets `topic_partitions` to the passed value.
    ///
    /// The partitions assigned to the member that can be used immediately.
    ///
    /// Supported API versions: 0-1
    pub fn with_topic_partitions(mut self, value: Vec<TopicPartitions>) -> Self {
        self.topic_partitions = value;
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
impl Encodable for Assignment {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.topic_partitions)?;
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
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.topic_partitions)?;
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

#[cfg(feature = "client")]
impl Decodable for Assignment {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let topic_partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            topic_partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for Assignment {
    fn default() -> Self {
        Self {
            topic_partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Assignment {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerGroupHeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-1
    pub throttle_time_ms: i32,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-1
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0-1
    pub error_message: Option<StrBytes>,

    /// The member id is generated by the consumer starting from version 1, while in version 0, it can be provided by users or generated by the group coordinator.
    ///
    /// Supported API versions: 0-1
    pub member_id: Option<StrBytes>,

    /// The member epoch.
    ///
    /// Supported API versions: 0-1
    pub member_epoch: i32,

    /// The heartbeat interval in milliseconds.
    ///
    /// Supported API versions: 0-1
    pub heartbeat_interval_ms: i32,

    /// null if not provided; the assignment otherwise.
    ///
    /// Supported API versions: 0-1
    pub assignment: Option<Assignment>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ConsumerGroupHeartbeatResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-1
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-1
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0-1
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member id is generated by the consumer starting from version 1, while in version 0, it can be provided by users or generated by the group coordinator.
    ///
    /// Supported API versions: 0-1
    pub fn with_member_id(mut self, value: Option<StrBytes>) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `member_epoch` to the passed value.
    ///
    /// The member epoch.
    ///
    /// Supported API versions: 0-1
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    /// Sets `heartbeat_interval_ms` to the passed value.
    ///
    /// The heartbeat interval in milliseconds.
    ///
    /// Supported API versions: 0-1
    pub fn with_heartbeat_interval_ms(mut self, value: i32) -> Self {
        self.heartbeat_interval_ms = value;
        self
    }
    /// Sets `assignment` to the passed value.
    ///
    /// null if not provided; the assignment otherwise.
    ///
    /// Supported API versions: 0-1
    pub fn with_assignment(mut self, value: Option<Assignment>) -> Self {
        self.assignment = value;
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
impl Encodable for ConsumerGroupHeartbeatResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.member_epoch)?;
        types::Int32.encode(buf, &self.heartbeat_interval_ms)?;
        types::OptionStruct { version }.encode(buf, &self.assignment)?;
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.member_epoch)?;
        total_size += types::Int32.compute_size(&self.heartbeat_interval_ms)?;
        total_size += types::OptionStruct { version }.compute_size(&self.assignment)?;
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

#[cfg(feature = "client")]
impl Decodable for ConsumerGroupHeartbeatResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let member_epoch = types::Int32.decode(buf)?;
        let heartbeat_interval_ms = types::Int32.decode(buf)?;
        let assignment = types::OptionStruct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            assignment,
            unknown_tagged_fields,
        })
    }
}

impl Default for ConsumerGroupHeartbeatResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: None,
            member_epoch: 0,
            heartbeat_interval_ms: 0,
            assignment: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ConsumerGroupHeartbeatResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitions {
    /// The topic ID.
    ///
    /// Supported API versions: 0-1
    pub topic_id: Uuid,

    /// The partitions.
    ///
    /// Supported API versions: 0-1
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicPartitions {
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic ID.
    ///
    /// Supported API versions: 0-1
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions.
    ///
    /// Supported API versions: 0-1
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
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
impl Encodable for TopicPartitions {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.topic_id)?;
        types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
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
        total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
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

#[cfg(feature = "client")]
impl Decodable for TopicPartitions {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = types::Uuid.decode(buf)?;
        let partitions = types::CompactArray(types::Int32).decode(buf)?;
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

impl Default for TopicPartitions {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicPartitions {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ConsumerGroupHeartbeatResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
