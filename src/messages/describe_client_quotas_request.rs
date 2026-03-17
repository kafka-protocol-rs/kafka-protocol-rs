//! DescribeClientQuotasRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeClientQuotasRequest.json).
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
pub struct ComponentData {
    /// The entity type that the filter component applies to.
    ///
    /// Supported API versions: 0-1
    pub entity_type: StrBytes,

    /// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
    ///
    /// Supported API versions: 0-1
    pub match_type: i8,

    /// The string to match against, or null if unused for the match type.
    ///
    /// Supported API versions: 0-1
    pub _match: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ComponentData {
    /// Sets `entity_type` to the passed value.
    ///
    /// The entity type that the filter component applies to.
    ///
    /// Supported API versions: 0-1
    pub fn with_entity_type(mut self, value: StrBytes) -> Self {
        self.entity_type = value;
        self
    }
    /// Sets `match_type` to the passed value.
    ///
    /// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
    ///
    /// Supported API versions: 0-1
    pub fn with_match_type(mut self, value: i8) -> Self {
        self.match_type = value;
        self
    }
    /// Sets `_match` to the passed value.
    ///
    /// The string to match against, or null if unused for the match type.
    ///
    /// Supported API versions: 0-1
    pub fn with_match(mut self, value: Option<StrBytes>) -> Self {
        self._match = value;
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
impl Encodable for ComponentData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::CompactString.encode(buf, &self.entity_type)?;
        } else {
            types::String.encode(buf, &self.entity_type)?;
        }
        types::Int8.encode(buf, &self.match_type)?;
        if version >= 1 {
            types::CompactString.encode(buf, &self._match)?;
        } else {
            types::String.encode(buf, &self._match)?;
        }
        if version >= 1 {
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
            total_size += types::CompactString.compute_size(&self.entity_type)?;
        } else {
            total_size += types::String.compute_size(&self.entity_type)?;
        }
        total_size += types::Int8.compute_size(&self.match_type)?;
        if version >= 1 {
            total_size += types::CompactString.compute_size(&self._match)?;
        } else {
            total_size += types::String.compute_size(&self._match)?;
        }
        if version >= 1 {
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
impl Decodable for ComponentData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let entity_type = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let match_type = types::Int8.decode(buf)?;
        let _match = if version >= 1 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            entity_type,
            match_type,
            _match,
            unknown_tagged_fields,
        })
    }
}

impl Default for ComponentData {
    fn default() -> Self {
        Self {
            entity_type: Default::default(),
            match_type: 0,
            _match: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ComponentData {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasRequest {
    /// Filter components to apply to quota entities.
    ///
    /// Supported API versions: 0-1
    pub components: Vec<ComponentData>,

    /// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
    ///
    /// Supported API versions: 0-1
    pub strict: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeClientQuotasRequest {
    /// Sets `components` to the passed value.
    ///
    /// Filter components to apply to quota entities.
    ///
    /// Supported API versions: 0-1
    pub fn with_components(mut self, value: Vec<ComponentData>) -> Self {
        self.components = value;
        self
    }
    /// Sets `strict` to the passed value.
    ///
    /// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
    ///
    /// Supported API versions: 0-1
    pub fn with_strict(mut self, value: bool) -> Self {
        self.strict = value;
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
impl Encodable for DescribeClientQuotasRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.components)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.components)?;
        }
        types::Boolean.encode(buf, &self.strict)?;
        if version >= 1 {
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
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.components)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.components)?;
        }
        total_size += types::Boolean.compute_size(&self.strict)?;
        if version >= 1 {
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
impl Decodable for DescribeClientQuotasRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let components = if version >= 1 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let strict = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 1 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            components,
            strict,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeClientQuotasRequest {
    fn default() -> Self {
        Self {
            components: Default::default(),
            strict: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeClientQuotasRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeClientQuotasRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 1 {
            2
        } else {
            1
        }
    }
}
