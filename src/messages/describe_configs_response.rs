//! DescribeConfigsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeConfigsResponse.json).
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

/// Valid versions: 1-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResourceResult {
    /// The configuration name.
    ///
    /// Supported API versions: 1-4
    pub name: StrBytes,

    /// The configuration value.
    ///
    /// Supported API versions: 1-4
    pub value: Option<StrBytes>,

    /// True if the configuration is read-only.
    ///
    /// Supported API versions: 1-4
    pub read_only: bool,

    /// The configuration source.
    ///
    /// Supported API versions: 1-4
    pub config_source: i8,

    /// True if this configuration is sensitive.
    ///
    /// Supported API versions: 1-4
    pub is_sensitive: bool,

    /// The synonyms for this configuration key.
    ///
    /// Supported API versions: 1-4
    pub synonyms: Vec<DescribeConfigsSynonym>,

    /// The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
    ///
    /// Supported API versions: 3-4
    pub config_type: i8,

    /// The configuration documentation.
    ///
    /// Supported API versions: 3-4
    pub documentation: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsResourceResult {
    /// Sets `name` to the passed value.
    ///
    /// The configuration name.
    ///
    /// Supported API versions: 1-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `value` to the passed value.
    ///
    /// The configuration value.
    ///
    /// Supported API versions: 1-4
    pub fn with_value(mut self, value: Option<StrBytes>) -> Self {
        self.value = value;
        self
    }
    /// Sets `read_only` to the passed value.
    ///
    /// True if the configuration is read-only.
    ///
    /// Supported API versions: 1-4
    pub fn with_read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    /// Sets `config_source` to the passed value.
    ///
    /// The configuration source.
    ///
    /// Supported API versions: 1-4
    pub fn with_config_source(mut self, value: i8) -> Self {
        self.config_source = value;
        self
    }
    /// Sets `is_sensitive` to the passed value.
    ///
    /// True if this configuration is sensitive.
    ///
    /// Supported API versions: 1-4
    pub fn with_is_sensitive(mut self, value: bool) -> Self {
        self.is_sensitive = value;
        self
    }
    /// Sets `synonyms` to the passed value.
    ///
    /// The synonyms for this configuration key.
    ///
    /// Supported API versions: 1-4
    pub fn with_synonyms(mut self, value: Vec<DescribeConfigsSynonym>) -> Self {
        self.synonyms = value;
        self
    }
    /// Sets `config_type` to the passed value.
    ///
    /// The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
    ///
    /// Supported API versions: 3-4
    pub fn with_config_type(mut self, value: i8) -> Self {
        self.config_type = value;
        self
    }
    /// Sets `documentation` to the passed value.
    ///
    /// The configuration documentation.
    ///
    /// Supported API versions: 3-4
    pub fn with_documentation(mut self, value: Option<StrBytes>) -> Self {
        self.documentation = value;
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
impl Encodable for DescribeConfigsResourceResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.value)?;
        } else {
            types::String.encode(buf, &self.value)?;
        }
        types::Boolean.encode(buf, &self.read_only)?;
        types::Int8.encode(buf, &self.config_source)?;
        types::Boolean.encode(buf, &self.is_sensitive)?;
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.synonyms)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.synonyms)?;
        }
        if version >= 3 {
            types::Int8.encode(buf, &self.config_type)?;
        }
        if version >= 3 {
            if version >= 4 {
                types::CompactString.encode(buf, &self.documentation)?;
            } else {
                types::String.encode(buf, &self.documentation)?;
            }
        }
        if version >= 4 {
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
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.value)?;
        } else {
            total_size += types::String.compute_size(&self.value)?;
        }
        total_size += types::Boolean.compute_size(&self.read_only)?;
        total_size += types::Int8.compute_size(&self.config_source)?;
        total_size += types::Boolean.compute_size(&self.is_sensitive)?;
        if version >= 4 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.synonyms)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.synonyms)?;
        }
        if version >= 3 {
            total_size += types::Int8.compute_size(&self.config_type)?;
        }
        if version >= 3 {
            if version >= 4 {
                total_size += types::CompactString.compute_size(&self.documentation)?;
            } else {
                total_size += types::String.compute_size(&self.documentation)?;
            }
        }
        if version >= 4 {
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
impl Decodable for DescribeConfigsResourceResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let value = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let read_only = types::Boolean.decode(buf)?;
        let config_source = types::Int8.decode(buf)?;
        let is_sensitive = types::Boolean.decode(buf)?;
        let synonyms = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let config_type = if version >= 3 {
            types::Int8.decode(buf)?
        } else {
            0
        };
        let documentation = if version >= 3 {
            if version >= 4 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Some(Default::default())
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            name,
            value,
            read_only,
            config_source,
            is_sensitive,
            synonyms,
            config_type,
            documentation,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsResourceResult {
    fn default() -> Self {
        Self {
            name: Default::default(),
            value: Some(Default::default()),
            read_only: false,
            config_source: -1,
            is_sensitive: false,
            synonyms: Default::default(),
            config_type: 0,
            documentation: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsResourceResult {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-4
    pub throttle_time_ms: i32,

    /// The results for each resource.
    ///
    /// Supported API versions: 1-4
    pub results: Vec<DescribeConfigsResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-4
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `results` to the passed value.
    ///
    /// The results for each resource.
    ///
    /// Supported API versions: 1-4
    pub fn with_results(mut self, value: Vec<DescribeConfigsResult>) -> Self {
        self.results = value;
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
impl Encodable for DescribeConfigsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.results)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.results)?;
        }
        if version >= 4 {
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
        if version >= 4 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.results)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.results)?;
        }
        if version >= 4 {
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
impl Decodable for DescribeConfigsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let results = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
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
            results,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            results: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResult {
    /// The error code, or 0 if we were able to successfully describe the configurations.
    ///
    /// Supported API versions: 1-4
    pub error_code: i16,

    /// The error message, or null if we were able to successfully describe the configurations.
    ///
    /// Supported API versions: 1-4
    pub error_message: Option<StrBytes>,

    /// The resource type.
    ///
    /// Supported API versions: 1-4
    pub resource_type: i8,

    /// The resource name.
    ///
    /// Supported API versions: 1-4
    pub resource_name: StrBytes,

    /// Each listed configuration.
    ///
    /// Supported API versions: 1-4
    pub configs: Vec<DescribeConfigsResourceResult>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsResult {
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if we were able to successfully describe the configurations.
    ///
    /// Supported API versions: 1-4
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if we were able to successfully describe the configurations.
    ///
    /// Supported API versions: 1-4
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `resource_type` to the passed value.
    ///
    /// The resource type.
    ///
    /// Supported API versions: 1-4
    pub fn with_resource_type(mut self, value: i8) -> Self {
        self.resource_type = value;
        self
    }
    /// Sets `resource_name` to the passed value.
    ///
    /// The resource name.
    ///
    /// Supported API versions: 1-4
    pub fn with_resource_name(mut self, value: StrBytes) -> Self {
        self.resource_name = value;
        self
    }
    /// Sets `configs` to the passed value.
    ///
    /// Each listed configuration.
    ///
    /// Supported API versions: 1-4
    pub fn with_configs(mut self, value: Vec<DescribeConfigsResourceResult>) -> Self {
        self.configs = value;
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
impl Encodable for DescribeConfigsResult {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 4 {
            types::CompactString.encode(buf, &self.error_message)?;
        } else {
            types::String.encode(buf, &self.error_message)?;
        }
        types::Int8.encode(buf, &self.resource_type)?;
        if version >= 4 {
            types::CompactString.encode(buf, &self.resource_name)?;
        } else {
            types::String.encode(buf, &self.resource_name)?;
        }
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.configs)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.configs)?;
        }
        if version >= 4 {
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
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.error_message)?;
        } else {
            total_size += types::String.compute_size(&self.error_message)?;
        }
        total_size += types::Int8.compute_size(&self.resource_type)?;
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.resource_name)?;
        } else {
            total_size += types::String.compute_size(&self.resource_name)?;
        }
        if version >= 4 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.configs)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.configs)?;
        }
        if version >= 4 {
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
impl Decodable for DescribeConfigsResult {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let resource_type = types::Int8.decode(buf)?;
        let resource_name = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let configs = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
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
            configs,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsResult {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: Some(Default::default()),
            resource_type: 0,
            resource_name: Default::default(),
            configs: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsResult {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsSynonym {
    /// The synonym name.
    ///
    /// Supported API versions: 1-4
    pub name: StrBytes,

    /// The synonym value.
    ///
    /// Supported API versions: 1-4
    pub value: Option<StrBytes>,

    /// The synonym source.
    ///
    /// Supported API versions: 1-4
    pub source: i8,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsSynonym {
    /// Sets `name` to the passed value.
    ///
    /// The synonym name.
    ///
    /// Supported API versions: 1-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `value` to the passed value.
    ///
    /// The synonym value.
    ///
    /// Supported API versions: 1-4
    pub fn with_value(mut self, value: Option<StrBytes>) -> Self {
        self.value = value;
        self
    }
    /// Sets `source` to the passed value.
    ///
    /// The synonym source.
    ///
    /// Supported API versions: 1-4
    pub fn with_source(mut self, value: i8) -> Self {
        self.source = value;
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
impl Encodable for DescribeConfigsSynonym {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.value)?;
        } else {
            types::String.encode(buf, &self.value)?;
        }
        types::Int8.encode(buf, &self.source)?;
        if version >= 4 {
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
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.value)?;
        } else {
            total_size += types::String.compute_size(&self.value)?;
        }
        total_size += types::Int8.compute_size(&self.source)?;
        if version >= 4 {
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
impl Decodable for DescribeConfigsSynonym {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let value = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let source = types::Int8.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 4 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            name,
            value,
            source,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsSynonym {
    fn default() -> Self {
        Self {
            name: Default::default(),
            value: Some(Default::default()),
            source: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsSynonym {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeConfigsResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 4 {
            1
        } else {
            0
        }
    }
}
