//! DescribeGroupsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeGroupsRequest.json).
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

/// Valid versions: 0-6
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    ///
    /// Supported API versions: 0-6
    pub groups: Vec<super::GroupId>,

    /// Whether to include authorized operations.
    ///
    /// Supported API versions: 3-6
    pub include_authorized_operations: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeGroupsRequest {
    /// Sets `groups` to the passed value.
    ///
    /// The names of the groups to describe.
    ///
    /// Supported API versions: 0-6
    pub fn with_groups(mut self, value: Vec<super::GroupId>) -> Self {
        self.groups = value;
        self
    }
    /// Sets `include_authorized_operations` to the passed value.
    ///
    /// Whether to include authorized operations.
    ///
    /// Supported API versions: 3-6
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
impl Encodable for DescribeGroupsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        if version >= 5 {
            types::CompactArray(types::CompactString).encode(buf, &self.groups)?;
        } else {
            types::Array(types::String).encode(buf, &self.groups)?;
        }
        if version >= 3 {
            types::Boolean.encode(buf, &self.include_authorized_operations)?;
        } else {
            if self.include_authorized_operations {
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
            total_size += types::CompactArray(types::CompactString).compute_size(&self.groups)?;
        } else {
            total_size += types::Array(types::String).compute_size(&self.groups)?;
        }
        if version >= 3 {
            total_size += types::Boolean.compute_size(&self.include_authorized_operations)?;
        } else {
            if self.include_authorized_operations {
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

#[cfg(feature = "broker")]
impl Decodable for DescribeGroupsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        let groups = if version >= 5 {
            types::CompactArray(types::CompactString).decode(buf)?
        } else {
            types::Array(types::String).decode(buf)?
        };
        let include_authorized_operations = if version >= 3 {
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
            groups,
            include_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeGroupsRequest {
    fn default() -> Self {
        Self {
            groups: Default::default(),
            include_authorized_operations: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeGroupsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeGroupsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 5 {
            2
        } else {
            1
        }
    }
}
