//! DefaultPrincipalData
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DefaultPrincipalData.json).
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

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DefaultPrincipalData {
    /// The principal type.
    ///
    /// Supported API versions: 0
    pub _type: StrBytes,

    /// The principal name.
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    ///
    /// Supported API versions: 0
    pub token_authenticated: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DefaultPrincipalData {
    /// Sets `_type` to the passed value.
    ///
    /// The principal type.
    ///
    /// Supported API versions: 0
    pub fn with_type(mut self, value: StrBytes) -> Self {
        self._type = value;
        self
    }
    /// Sets `name` to the passed value.
    ///
    /// The principal name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `token_authenticated` to the passed value.
    ///
    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    ///
    /// Supported API versions: 0
    pub fn with_token_authenticated(mut self, value: bool) -> Self {
        self.token_authenticated = value;
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

impl Encodable for DefaultPrincipalData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self._type)?;
        types::CompactString.encode(buf, &self.name)?;
        types::Boolean.encode(buf, &self.token_authenticated)?;
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
        total_size += types::CompactString.compute_size(&self._type)?;
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Boolean.compute_size(&self.token_authenticated)?;
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

impl Decodable for DefaultPrincipalData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let _type = types::CompactString.decode(buf)?;
        let name = types::CompactString.decode(buf)?;
        let token_authenticated = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            _type,
            name,
            token_authenticated,
            unknown_tagged_fields,
        })
    }
}

impl Default for DefaultPrincipalData {
    fn default() -> Self {
        Self {
            _type: Default::default(),
            name: Default::default(),
            token_authenticated: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DefaultPrincipalData {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
