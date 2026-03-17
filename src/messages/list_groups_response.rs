//! ListGroupsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ListGroupsResponse.json).
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

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ListGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-5
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-5
    pub error_code: i16,

    /// Each group in the response.
    ///
    /// Supported API versions: 0-5
    pub groups: Vec<ListedGroup>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ListGroupsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-5
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-5
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `groups` to the passed value.
    ///
    /// Each group in the response.
    ///
    /// Supported API versions: 0-5
    pub fn with_groups(mut self, value: Vec<ListedGroup>) -> Self {
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
impl Encodable for ListGroupsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 3 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.groups)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.groups)?;
        }
        if version >= 3 {
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
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 3 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.groups)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.groups)?;
        }
        if version >= 3 {
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
impl Decodable for ListGroupsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let error_code = types::Int16.decode(buf)?;
        let groups = if version >= 3 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
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
            error_code,
            groups,
            unknown_tagged_fields,
        })
    }
}

impl Default for ListGroupsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            groups: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ListGroupsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ListedGroup {
    /// The group ID.
    ///
    /// Supported API versions: 0-5
    pub group_id: super::GroupId,

    /// The group protocol type.
    ///
    /// Supported API versions: 0-5
    pub protocol_type: StrBytes,

    /// The group state name.
    ///
    /// Supported API versions: 4-5
    pub group_state: StrBytes,

    /// The group type name.
    ///
    /// Supported API versions: 5
    pub group_type: StrBytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ListedGroup {
    /// Sets `group_id` to the passed value.
    ///
    /// The group ID.
    ///
    /// Supported API versions: 0-5
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `protocol_type` to the passed value.
    ///
    /// The group protocol type.
    ///
    /// Supported API versions: 0-5
    pub fn with_protocol_type(mut self, value: StrBytes) -> Self {
        self.protocol_type = value;
        self
    }
    /// Sets `group_state` to the passed value.
    ///
    /// The group state name.
    ///
    /// Supported API versions: 4-5
    pub fn with_group_state(mut self, value: StrBytes) -> Self {
        self.group_state = value;
        self
    }
    /// Sets `group_type` to the passed value.
    ///
    /// The group type name.
    ///
    /// Supported API versions: 5
    pub fn with_group_type(mut self, value: StrBytes) -> Self {
        self.group_type = value;
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
impl Encodable for ListedGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.group_id)?;
        } else {
            types::String.encode(buf, &self.group_id)?;
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.protocol_type)?;
        } else {
            types::String.encode(buf, &self.protocol_type)?;
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.group_state)?;
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.group_type)?;
        }
        if version >= 3 {
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
            total_size += types::CompactString.compute_size(&self.group_id)?;
        } else {
            total_size += types::String.compute_size(&self.group_id)?;
        }
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.protocol_type)?;
        } else {
            total_size += types::String.compute_size(&self.protocol_type)?;
        }
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.group_state)?;
        }
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.group_type)?;
        }
        if version >= 3 {
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
impl Decodable for ListedGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let protocol_type = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let group_state = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let group_type = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
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
            protocol_type,
            group_state,
            group_type,
            unknown_tagged_fields,
        })
    }
}

impl Default for ListedGroup {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            protocol_type: Default::default(),
            group_state: Default::default(),
            group_type: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ListedGroup {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ListGroupsResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 3 {
            1
        } else {
            0
        }
    }
}
