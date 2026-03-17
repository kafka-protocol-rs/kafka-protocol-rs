//! ListGroupsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ListGroupsRequest.json).
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
pub struct ListGroupsRequest {
    /// The states of the groups we want to list. If empty, all groups are returned with their state.
    ///
    /// Supported API versions: 4-5
    pub states_filter: Vec<StrBytes>,

    /// The types of the groups we want to list. If empty, all groups are returned with their type.
    ///
    /// Supported API versions: 5
    pub types_filter: Vec<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ListGroupsRequest {
    /// Sets `states_filter` to the passed value.
    ///
    /// The states of the groups we want to list. If empty, all groups are returned with their state.
    ///
    /// Supported API versions: 4-5
    pub fn with_states_filter(mut self, value: Vec<StrBytes>) -> Self {
        self.states_filter = value;
        self
    }
    /// Sets `types_filter` to the passed value.
    ///
    /// The types of the groups we want to list. If empty, all groups are returned with their type.
    ///
    /// Supported API versions: 5
    pub fn with_types_filter(mut self, value: Vec<StrBytes>) -> Self {
        self.types_filter = value;
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
impl Encodable for ListGroupsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactArray(types::CompactString).encode(buf, &self.states_filter)?;
        } else {
            if !self.states_filter.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            types::CompactArray(types::CompactString).encode(buf, &self.types_filter)?;
        } else {
            if !self.types_filter.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
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
        if version >= 4 {
            total_size +=
                types::CompactArray(types::CompactString).compute_size(&self.states_filter)?;
        } else {
            if !self.states_filter.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            total_size +=
                types::CompactArray(types::CompactString).compute_size(&self.types_filter)?;
        } else {
            if !self.types_filter.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
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

#[cfg(feature = "broker")]
impl Decodable for ListGroupsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let states_filter = if version >= 4 {
            types::CompactArray(types::CompactString).decode(buf)?
        } else {
            Default::default()
        };
        let types_filter = if version >= 5 {
            types::CompactArray(types::CompactString).decode(buf)?
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
            states_filter,
            types_filter,
            unknown_tagged_fields,
        })
    }
}

impl Default for ListGroupsRequest {
    fn default() -> Self {
        Self {
            states_filter: Default::default(),
            types_filter: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ListGroupsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ListGroupsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 3 {
            2
        } else {
            1
        }
    }
}
