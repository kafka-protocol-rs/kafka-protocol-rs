//! DeleteAclsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DeleteAclsRequest.json).
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

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAclsFilter {
    /// The resource type.
    ///
    /// Supported API versions: 1-3
    pub resource_type_filter: i8,

    /// The resource name.
    ///
    /// Supported API versions: 1-3
    pub resource_name_filter: Option<StrBytes>,

    /// The pattern type.
    ///
    /// Supported API versions: 1-3
    pub pattern_type_filter: i8,

    /// The principal filter, or null to accept all principals.
    ///
    /// Supported API versions: 1-3
    pub principal_filter: Option<StrBytes>,

    /// The host filter, or null to accept all hosts.
    ///
    /// Supported API versions: 1-3
    pub host_filter: Option<StrBytes>,

    /// The ACL operation.
    ///
    /// Supported API versions: 1-3
    pub operation: i8,

    /// The permission type.
    ///
    /// Supported API versions: 1-3
    pub permission_type: i8,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteAclsFilter {
    /// Sets `resource_type_filter` to the passed value.
    ///
    /// The resource type.
    ///
    /// Supported API versions: 1-3
    pub fn with_resource_type_filter(mut self, value: i8) -> Self {
        self.resource_type_filter = value;
        self
    }
    /// Sets `resource_name_filter` to the passed value.
    ///
    /// The resource name.
    ///
    /// Supported API versions: 1-3
    pub fn with_resource_name_filter(mut self, value: Option<StrBytes>) -> Self {
        self.resource_name_filter = value;
        self
    }
    /// Sets `pattern_type_filter` to the passed value.
    ///
    /// The pattern type.
    ///
    /// Supported API versions: 1-3
    pub fn with_pattern_type_filter(mut self, value: i8) -> Self {
        self.pattern_type_filter = value;
        self
    }
    /// Sets `principal_filter` to the passed value.
    ///
    /// The principal filter, or null to accept all principals.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_filter(mut self, value: Option<StrBytes>) -> Self {
        self.principal_filter = value;
        self
    }
    /// Sets `host_filter` to the passed value.
    ///
    /// The host filter, or null to accept all hosts.
    ///
    /// Supported API versions: 1-3
    pub fn with_host_filter(mut self, value: Option<StrBytes>) -> Self {
        self.host_filter = value;
        self
    }
    /// Sets `operation` to the passed value.
    ///
    /// The ACL operation.
    ///
    /// Supported API versions: 1-3
    pub fn with_operation(mut self, value: i8) -> Self {
        self.operation = value;
        self
    }
    /// Sets `permission_type` to the passed value.
    ///
    /// The permission type.
    ///
    /// Supported API versions: 1-3
    pub fn with_permission_type(mut self, value: i8) -> Self {
        self.permission_type = value;
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
impl Encodable for DeleteAclsFilter {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int8.encode(buf, &self.resource_type_filter)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.resource_name_filter)?;
        } else {
            types::String.encode(buf, &self.resource_name_filter)?;
        }
        types::Int8.encode(buf, &self.pattern_type_filter)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.principal_filter)?;
        } else {
            types::String.encode(buf, &self.principal_filter)?;
        }
        if version >= 2 {
            types::CompactString.encode(buf, &self.host_filter)?;
        } else {
            types::String.encode(buf, &self.host_filter)?;
        }
        types::Int8.encode(buf, &self.operation)?;
        types::Int8.encode(buf, &self.permission_type)?;
        if version >= 2 {
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
        total_size += types::Int8.compute_size(&self.resource_type_filter)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.resource_name_filter)?;
        } else {
            total_size += types::String.compute_size(&self.resource_name_filter)?;
        }
        total_size += types::Int8.compute_size(&self.pattern_type_filter)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.principal_filter)?;
        } else {
            total_size += types::String.compute_size(&self.principal_filter)?;
        }
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.host_filter)?;
        } else {
            total_size += types::String.compute_size(&self.host_filter)?;
        }
        total_size += types::Int8.compute_size(&self.operation)?;
        total_size += types::Int8.compute_size(&self.permission_type)?;
        if version >= 2 {
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
impl Decodable for DeleteAclsFilter {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let resource_type_filter = types::Int8.decode(buf)?;
        let resource_name_filter = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let pattern_type_filter = types::Int8.decode(buf)?;
        let principal_filter = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let host_filter = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let operation = types::Int8.decode(buf)?;
        let permission_type = types::Int8.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 2 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteAclsFilter {
    fn default() -> Self {
        Self {
            resource_type_filter: 0,
            resource_name_filter: Some(Default::default()),
            pattern_type_filter: 3,
            principal_filter: Some(Default::default()),
            host_filter: Some(Default::default()),
            operation: 0,
            permission_type: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteAclsFilter {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAclsRequest {
    /// The filters to use when deleting ACLs.
    ///
    /// Supported API versions: 1-3
    pub filters: Vec<DeleteAclsFilter>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteAclsRequest {
    /// Sets `filters` to the passed value.
    ///
    /// The filters to use when deleting ACLs.
    ///
    /// Supported API versions: 1-3
    pub fn with_filters(mut self, value: Vec<DeleteAclsFilter>) -> Self {
        self.filters = value;
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
impl Encodable for DeleteAclsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        if version >= 2 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.filters)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.filters)?;
        }
        if version >= 2 {
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
        if version >= 2 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.filters)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.filters)?;
        }
        if version >= 2 {
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
impl Decodable for DeleteAclsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let filters = if version >= 2 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 2 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            filters,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteAclsRequest {
    fn default() -> Self {
        Self {
            filters: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteAclsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DeleteAclsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            2
        } else {
            1
        }
    }
}
