//! ShareGroupDescribeRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ShareGroupDescribeRequest.json).
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
pub struct ShareGroupDescribeRequest {
    /// The ids of the groups to describe.
    ///
    /// Supported API versions: 1
    pub group_ids: Vec<super::GroupId>,

    /// Whether to include authorized operations.
    ///
    /// Supported API versions: 1
    pub include_authorized_operations: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ShareGroupDescribeRequest {
    /// Sets `group_ids` to the passed value.
    ///
    /// The ids of the groups to describe.
    ///
    /// Supported API versions: 1
    pub fn with_group_ids(mut self, value: Vec<super::GroupId>) -> Self {
        self.group_ids = value;
        self
    }
    /// Sets `include_authorized_operations` to the passed value.
    ///
    /// Whether to include authorized operations.
    ///
    /// Supported API versions: 1
    pub fn with_include_authorized_operations(mut self, value: bool) -> Self {
        self.include_authorized_operations = value;
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
impl Encodable for ShareGroupDescribeRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::CompactString).encode(buf, &self.group_ids)?;
        types::Boolean.encode(buf, &self.include_authorized_operations)?;
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
        total_size += types::CompactArray(types::CompactString).compute_size(&self.group_ids)?;
        total_size += types::Boolean.compute_size(&self.include_authorized_operations)?;
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
impl Decodable for ShareGroupDescribeRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let group_ids = types::CompactArray(types::CompactString).decode(buf)?;
        let include_authorized_operations = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            group_ids,
            include_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for ShareGroupDescribeRequest {
    fn default() -> Self {
        Self {
            group_ids: Default::default(),
            include_authorized_operations: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ShareGroupDescribeRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ShareGroupDescribeRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
