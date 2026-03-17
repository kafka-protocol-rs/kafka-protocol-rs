//! DeleteAclsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DeleteAclsResponse.json).
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
pub struct DeleteAclsFilterResult {
    /// The error code, or 0 if the filter succeeded.
    ///
    /// Supported API versions: 1-3
    pub error_code: i16,

    /// The error message, or null if the filter succeeded.
    ///
    /// Supported API versions: 1-3
    pub error_message: Option<StrBytes>,

    /// The ACLs which matched this filter.
    ///
    /// Supported API versions: 1-3
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteAclsFilterResult {
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if the filter succeeded.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if the filter succeeded.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `matching_acls` to the passed value.
    ///
    /// The ACLs which matched this filter.
    ///
    /// Supported API versions: 1-3
    pub fn with_matching_acls(mut self, value: Vec<DeleteAclsMatchingAcl>) -> Self {
        self.matching_acls = value;
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
impl Encodable for DeleteAclsFilterResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.error_message)?;
        } else {
            types::String.encode(buf, &self.error_message)?;
        }
        if version >= 2 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.matching_acls)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.matching_acls)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.error_message)?;
        } else {
            total_size += types::String.compute_size(&self.error_message)?;
        }
        if version >= 2 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.matching_acls)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.matching_acls)?;
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

#[cfg(feature = "client")]
impl Decodable for DeleteAclsFilterResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let matching_acls = if version >= 2 {
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
            error_code,
            error_message,
            matching_acls,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteAclsFilterResult {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: Some(Default::default()),
            matching_acls: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteAclsFilterResult {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAclsMatchingAcl {
    /// The deletion error code, or 0 if the deletion succeeded.
    ///
    /// Supported API versions: 1-3
    pub error_code: i16,

    /// The deletion error message, or null if the deletion succeeded.
    ///
    /// Supported API versions: 1-3
    pub error_message: Option<StrBytes>,

    /// The ACL resource type.
    ///
    /// Supported API versions: 1-3
    pub resource_type: i8,

    /// The ACL resource name.
    ///
    /// Supported API versions: 1-3
    pub resource_name: StrBytes,

    /// The ACL resource pattern type.
    ///
    /// Supported API versions: 1-3
    pub pattern_type: i8,

    /// The ACL principal.
    ///
    /// Supported API versions: 1-3
    pub principal: StrBytes,

    /// The ACL host.
    ///
    /// Supported API versions: 1-3
    pub host: StrBytes,

    /// The ACL operation.
    ///
    /// Supported API versions: 1-3
    pub operation: i8,

    /// The ACL permission type.
    ///
    /// Supported API versions: 1-3
    pub permission_type: i8,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteAclsMatchingAcl {
    /// Sets `error_code` to the passed value.
    ///
    /// The deletion error code, or 0 if the deletion succeeded.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The deletion error message, or null if the deletion succeeded.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `resource_type` to the passed value.
    ///
    /// The ACL resource type.
    ///
    /// Supported API versions: 1-3
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    /// Sets `resource_name` to the passed value.
    ///
    /// The ACL resource name.
    ///
    /// Supported API versions: 1-3
    pub fn with_resource_name(mut self, value: StrBytes) -> Self {
        self.resource_name = value;
        self
    }
    /// Sets `pattern_type` to the passed value.
    ///
    /// The ACL resource pattern type.
    ///
    /// Supported API versions: 1-3
    pub fn with_pattern_type(mut self, value: i8) -> Self {
        self.pattern_type = value;
        self
    }
    /// Sets `principal` to the passed value.
    ///
    /// The ACL principal.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal(mut self, value: StrBytes) -> Self {
        self.principal = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The ACL host.
    ///
    /// Supported API versions: 1-3
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
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
    /// The ACL permission type.
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

#[cfg(feature = "broker")]
impl Encodable for DeleteAclsMatchingAcl {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.error_message)?;
        } else {
            types::String.encode(buf, &self.error_message)?;
        }
        types::Int8.encode(buf, &self.resource_type)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.resource_name)?;
        } else {
            types::String.encode(buf, &self.resource_name)?;
        }
        types::Int8.encode(buf, &self.pattern_type)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.principal)?;
        } else {
            types::String.encode(buf, &self.principal)?;
        }
        if version >= 2 {
            types::CompactString.encode(buf, &self.host)?;
        } else {
            types::String.encode(buf, &self.host)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.error_message)?;
        } else {
            total_size += types::String.compute_size(&self.error_message)?;
        }
        total_size += types::Int8.compute_size(&self.resource_type)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.resource_name)?;
        } else {
            total_size += types::String.compute_size(&self.resource_name)?;
        }
        total_size += types::Int8.compute_size(&self.pattern_type)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.principal)?;
        } else {
            total_size += types::String.compute_size(&self.principal)?;
        }
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.host)?;
        } else {
            total_size += types::String.compute_size(&self.host)?;
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

#[cfg(feature = "client")]
impl Decodable for DeleteAclsMatchingAcl {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let resource_type = types::Int8.decode(buf)?;
        let resource_name = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let pattern_type = types::Int8.decode(buf)?;
        let principal = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let host = if version >= 2 {
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
            error_code,
            error_message,
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteAclsMatchingAcl {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: Some(Default::default()),
            resource_type: 0,
            resource_name: Default::default(),
            pattern_type: 3,
            principal: Default::default(),
            host: Default::default(),
            operation: 0,
            permission_type: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteAclsMatchingAcl {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAclsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-3
    pub throttle_time_ms: i32,

    /// The results for each filter.
    ///
    /// Supported API versions: 1-3
    pub filter_results: Vec<DeleteAclsFilterResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DeleteAclsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-3
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `filter_results` to the passed value.
    ///
    /// The results for each filter.
    ///
    /// Supported API versions: 1-3
    pub fn with_filter_results(mut self, value: Vec<DeleteAclsFilterResult>) -> Self {
        self.filter_results = value;
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
impl Encodable for DeleteAclsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        if version >= 2 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.filter_results)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.filter_results)?;
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        if version >= 2 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.filter_results)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.filter_results)?;
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

#[cfg(feature = "client")]
impl Decodable for DeleteAclsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let filter_results = if version >= 2 {
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
            throttle_time_ms,
            filter_results,
            unknown_tagged_fields,
        })
    }
}

impl Default for DeleteAclsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            filter_results: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DeleteAclsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DeleteAclsResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
