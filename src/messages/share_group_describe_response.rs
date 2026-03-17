//! ShareGroupDescribeResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ShareGroupDescribeResponse.json).
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

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// The assigned topic-partitions to the member.
    ///
    /// Supported API versions: 1
    pub topic_partitions: Vec<TopicPartitions>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Assignment {
    /// Sets `topic_partitions` to the passed value.
    ///
    /// The assigned topic-partitions to the member.
    ///
    /// Supported API versions: 1
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
        if version != 1 {
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
        if version != 1 {
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
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    ///
    /// Supported API versions: 1
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub error_message: Option<StrBytes>,

    /// The group ID string.
    ///
    /// Supported API versions: 1
    pub group_id: super::GroupId,

    /// The group state string, or the empty string.
    ///
    /// Supported API versions: 1
    pub group_state: StrBytes,

    /// The group epoch.
    ///
    /// Supported API versions: 1
    pub group_epoch: i32,

    /// The assignment epoch.
    ///
    /// Supported API versions: 1
    pub assignment_epoch: i32,

    /// The selected assignor.
    ///
    /// Supported API versions: 1
    pub assignor_name: StrBytes,

    /// The members.
    ///
    /// Supported API versions: 1
    pub members: Vec<Member>,

    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Supported API versions: 1
    pub authorized_operations: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribedGroup {
    /// Sets `error_code` to the passed value.
    ///
    /// The describe error, or 0 if there was no error.
    ///
    /// Supported API versions: 1
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 1
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `group_id` to the passed value.
    ///
    /// The group ID string.
    ///
    /// Supported API versions: 1
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `group_state` to the passed value.
    ///
    /// The group state string, or the empty string.
    ///
    /// Supported API versions: 1
    pub fn with_group_state(mut self, value: StrBytes) -> Self {
        self.group_state = value;
        self
    }
    /// Sets `group_epoch` to the passed value.
    ///
    /// The group epoch.
    ///
    /// Supported API versions: 1
    pub fn with_group_epoch(mut self, value: i32) -> Self {
        self.group_epoch = value;
        self
    }
    /// Sets `assignment_epoch` to the passed value.
    ///
    /// The assignment epoch.
    ///
    /// Supported API versions: 1
    pub fn with_assignment_epoch(mut self, value: i32) -> Self {
        self.assignment_epoch = value;
        self
    }
    /// Sets `assignor_name` to the passed value.
    ///
    /// The selected assignor.
    ///
    /// Supported API versions: 1
    pub fn with_assignor_name(mut self, value: StrBytes) -> Self {
        self.assignor_name = value;
        self
    }
    /// Sets `members` to the passed value.
    ///
    /// The members.
    ///
    /// Supported API versions: 1
    pub fn with_members(mut self, value: Vec<Member>) -> Self {
        self.members = value;
        self
    }
    /// Sets `authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Supported API versions: 1
    pub fn with_authorized_operations(mut self, value: i32) -> Self {
        self.authorized_operations = value;
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
impl Encodable for DescribedGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.group_state)?;
        types::Int32.encode(buf, &self.group_epoch)?;
        types::Int32.encode(buf, &self.assignment_epoch)?;
        types::CompactString.encode(buf, &self.assignor_name)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.members)?;
        types::Int32.encode(buf, &self.authorized_operations)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::CompactString.compute_size(&self.group_id)?;
        total_size += types::CompactString.compute_size(&self.group_state)?;
        total_size += types::Int32.compute_size(&self.group_epoch)?;
        total_size += types::Int32.compute_size(&self.assignment_epoch)?;
        total_size += types::CompactString.compute_size(&self.assignor_name)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.members)?;
        total_size += types::Int32.compute_size(&self.authorized_operations)?;
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
impl Decodable for DescribedGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let group_id = types::CompactString.decode(buf)?;
        let group_state = types::CompactString.decode(buf)?;
        let group_epoch = types::Int32.decode(buf)?;
        let assignment_epoch = types::Int32.decode(buf)?;
        let assignor_name = types::CompactString.decode(buf)?;
        let members = types::CompactArray(types::Struct { version }).decode(buf)?;
        let authorized_operations = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribedGroup {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: None,
            group_id: Default::default(),
            group_state: Default::default(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: Default::default(),
            members: Default::default(),
            authorized_operations: -2147483648,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribedGroup {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// The member ID.
    ///
    /// Supported API versions: 1
    pub member_id: StrBytes,

    /// The member rack ID.
    ///
    /// Supported API versions: 1
    pub rack_id: Option<StrBytes>,

    /// The current member epoch.
    ///
    /// Supported API versions: 1
    pub member_epoch: i32,

    /// The client ID.
    ///
    /// Supported API versions: 1
    pub client_id: StrBytes,

    /// The client host.
    ///
    /// Supported API versions: 1
    pub client_host: StrBytes,

    /// The subscribed topic names.
    ///
    /// Supported API versions: 1
    pub subscribed_topic_names: Vec<super::TopicName>,

    /// The current assignment.
    ///
    /// Supported API versions: 1
    pub assignment: Assignment,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Member {
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID.
    ///
    /// Supported API versions: 1
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `rack_id` to the passed value.
    ///
    /// The member rack ID.
    ///
    /// Supported API versions: 1
    pub fn with_rack_id(mut self, value: Option<StrBytes>) -> Self {
        self.rack_id = value;
        self
    }
    /// Sets `member_epoch` to the passed value.
    ///
    /// The current member epoch.
    ///
    /// Supported API versions: 1
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    /// Sets `client_id` to the passed value.
    ///
    /// The client ID.
    ///
    /// Supported API versions: 1
    pub fn with_client_id(mut self, value: StrBytes) -> Self {
        self.client_id = value;
        self
    }
    /// Sets `client_host` to the passed value.
    ///
    /// The client host.
    ///
    /// Supported API versions: 1
    pub fn with_client_host(mut self, value: StrBytes) -> Self {
        self.client_host = value;
        self
    }
    /// Sets `subscribed_topic_names` to the passed value.
    ///
    /// The subscribed topic names.
    ///
    /// Supported API versions: 1
    pub fn with_subscribed_topic_names(mut self, value: Vec<super::TopicName>) -> Self {
        self.subscribed_topic_names = value;
        self
    }
    /// Sets `assignment` to the passed value.
    ///
    /// The current assignment.
    ///
    /// Supported API versions: 1
    pub fn with_assignment(mut self, value: Assignment) -> Self {
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
impl Encodable for Member {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.member_id)?;
        types::CompactString.encode(buf, &self.rack_id)?;
        types::Int32.encode(buf, &self.member_epoch)?;
        types::CompactString.encode(buf, &self.client_id)?;
        types::CompactString.encode(buf, &self.client_host)?;
        types::CompactArray(types::CompactString).encode(buf, &self.subscribed_topic_names)?;
        types::Struct { version }.encode(buf, &self.assignment)?;
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
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::CompactString.compute_size(&self.rack_id)?;
        total_size += types::Int32.compute_size(&self.member_epoch)?;
        total_size += types::CompactString.compute_size(&self.client_id)?;
        total_size += types::CompactString.compute_size(&self.client_host)?;
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.subscribed_topic_names)?;
        total_size += types::Struct { version }.compute_size(&self.assignment)?;
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
impl Decodable for Member {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let member_id = types::CompactString.decode(buf)?;
        let rack_id = types::CompactString.decode(buf)?;
        let member_epoch = types::Int32.decode(buf)?;
        let client_id = types::CompactString.decode(buf)?;
        let client_host = types::CompactString.decode(buf)?;
        let subscribed_topic_names = types::CompactArray(types::CompactString).decode(buf)?;
        let assignment = types::Struct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            member_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            assignment,
            unknown_tagged_fields,
        })
    }
}

impl Default for Member {
    fn default() -> Self {
        Self {
            member_id: Default::default(),
            rack_id: None,
            member_epoch: 0,
            client_id: Default::default(),
            client_host: Default::default(),
            subscribed_topic_names: Default::default(),
            assignment: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Member {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ShareGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1
    pub throttle_time_ms: i32,

    /// Each described group.
    ///
    /// Supported API versions: 1
    pub groups: Vec<DescribedGroup>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareGroupDescribeResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `groups` to the passed value.
    ///
    /// Each described group.
    ///
    /// Supported API versions: 1
    pub fn with_groups(mut self, value: Vec<DescribedGroup>) -> Self {
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
impl Encodable for ShareGroupDescribeResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.groups)?;
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
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.groups)?;
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
impl Decodable for ShareGroupDescribeResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let groups = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            groups,
            unknown_tagged_fields,
        })
    }
}

impl Default for ShareGroupDescribeResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            groups: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareGroupDescribeResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitions {
    /// The topic ID.
    ///
    /// Supported API versions: 1
    pub topic_id: Uuid,

    /// The topic name.
    ///
    /// Supported API versions: 1
    pub topic_name: super::TopicName,

    /// The partitions.
    ///
    /// Supported API versions: 1
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicPartitions {
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic ID.
    ///
    /// Supported API versions: 1
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `topic_name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 1
    pub fn with_topic_name(mut self, value: super::TopicName) -> Self {
        self.topic_name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions.
    ///
    /// Supported API versions: 1
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
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.topic_id)?;
        types::CompactString.encode(buf, &self.topic_name)?;
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
        total_size += types::CompactString.compute_size(&self.topic_name)?;
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
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = types::Uuid.decode(buf)?;
        let topic_name = types::CompactString.decode(buf)?;
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
            topic_name,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicPartitions {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            topic_name: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicPartitions {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ShareGroupDescribeResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
