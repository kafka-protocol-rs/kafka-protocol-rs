//! DescribeConfigsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeConfigsRequest.json).
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
pub struct DescribeConfigsRequest {
    /// The resources whose configurations we want to describe.
    ///
    /// Supported API versions: 1-4
    pub resources: Vec<DescribeConfigsResource>,

    /// True if we should include all synonyms.
    ///
    /// Supported API versions: 1-4
    pub include_synonyms: bool,

    /// True if we should include configuration documentation.
    ///
    /// Supported API versions: 3-4
    pub include_documentation: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsRequest {
    /// Sets `resources` to the passed value.
    ///
    /// The resources whose configurations we want to describe.
    ///
    /// Supported API versions: 1-4
    pub fn with_resources(mut self, value: Vec<DescribeConfigsResource>) -> Self {
        self.resources = value;
        self
    }
    /// Sets `include_synonyms` to the passed value.
    ///
    /// True if we should include all synonyms.
    ///
    /// Supported API versions: 1-4
    pub fn with_include_synonyms(mut self, value: bool) -> Self {
        self.include_synonyms = value;
        self
    }
    /// Sets `include_documentation` to the passed value.
    ///
    /// True if we should include configuration documentation.
    ///
    /// Supported API versions: 3-4
    pub fn with_include_documentation(mut self, value: bool) -> Self {
        self.include_documentation = value;
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
impl Encodable for DescribeConfigsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.resources)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.resources)?;
        }
        types::Boolean.encode(buf, &self.include_synonyms)?;
        if version >= 3 {
            types::Boolean.encode(buf, &self.include_documentation)?;
        } else {
            if self.include_documentation {
                bail!("A field is set that is not available on the selected protocol version");
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
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.resources)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.resources)?;
        }
        total_size += types::Boolean.compute_size(&self.include_synonyms)?;
        if version >= 3 {
            total_size += types::Boolean.compute_size(&self.include_documentation)?;
        } else {
            if self.include_documentation {
                bail!("A field is set that is not available on the selected protocol version");
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

#[cfg(feature = "broker")]
impl Decodable for DescribeConfigsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let resources = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let include_synonyms = types::Boolean.decode(buf)?;
        let include_documentation = if version >= 3 {
            types::Boolean.decode(buf)?
        } else {
            false
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
            resources,
            include_synonyms,
            include_documentation,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsRequest {
    fn default() -> Self {
        Self {
            resources: Default::default(),
            include_synonyms: false,
            include_documentation: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeConfigsResource {
    /// The resource type.
    ///
    /// Supported API versions: 1-4
    pub resource_type: i8,

    /// The resource name.
    ///
    /// Supported API versions: 1-4
    pub resource_name: StrBytes,

    /// The configuration keys to list, or null to list all configuration keys.
    ///
    /// Supported API versions: 1-4
    pub configuration_keys: Option<Vec<StrBytes>>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeConfigsResource {
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
    /// Sets `configuration_keys` to the passed value.
    ///
    /// The configuration keys to list, or null to list all configuration keys.
    ///
    /// Supported API versions: 1-4
    pub fn with_configuration_keys(mut self, value: Option<Vec<StrBytes>>) -> Self {
        self.configuration_keys = value;
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
impl Encodable for DescribeConfigsResource {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int8.encode(buf, &self.resource_type)?;
        if version >= 4 {
            types::CompactString.encode(buf, &self.resource_name)?;
        } else {
            types::String.encode(buf, &self.resource_name)?;
        }
        if version >= 4 {
            types::CompactArray(types::CompactString).encode(buf, &self.configuration_keys)?;
        } else {
            types::Array(types::String).encode(buf, &self.configuration_keys)?;
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
        total_size += types::Int8.compute_size(&self.resource_type)?;
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.resource_name)?;
        } else {
            total_size += types::String.compute_size(&self.resource_name)?;
        }
        if version >= 4 {
            total_size +=
                types::CompactArray(types::CompactString).compute_size(&self.configuration_keys)?;
        } else {
            total_size += types::Array(types::String).compute_size(&self.configuration_keys)?;
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

#[cfg(feature = "broker")]
impl Decodable for DescribeConfigsResource {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let resource_type = types::Int8.decode(buf)?;
        let resource_name = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let configuration_keys = if version >= 4 {
            types::CompactArray(types::CompactString).decode(buf)?
        } else {
            types::Array(types::String).decode(buf)?
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
            resource_type,
            resource_name,
            configuration_keys,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeConfigsResource {
    fn default() -> Self {
        Self {
            resource_type: 0,
            resource_name: Default::default(),
            configuration_keys: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeConfigsResource {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeConfigsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 4 {
            2
        } else {
            1
        }
    }
}
