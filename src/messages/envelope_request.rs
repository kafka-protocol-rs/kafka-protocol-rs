//! EnvelopeRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/EnvelopeRequest.json).
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
pub struct EnvelopeRequest {
    /// The embedded request header and data.
    ///
    /// Supported API versions: 0
    pub request_data: Bytes,

    /// Value of the initial client principal when the request is redirected by a broker.
    ///
    /// Supported API versions: 0
    pub request_principal: Option<Bytes>,

    /// The original client's address in bytes.
    ///
    /// Supported API versions: 0
    pub client_host_address: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl EnvelopeRequest {
    /// Sets `request_data` to the passed value.
    ///
    /// The embedded request header and data.
    ///
    /// Supported API versions: 0
    pub fn with_request_data(mut self, value: Bytes) -> Self {
        self.request_data = value;
        self
    }
    /// Sets `request_principal` to the passed value.
    ///
    /// Value of the initial client principal when the request is redirected by a broker.
    ///
    /// Supported API versions: 0
    pub fn with_request_principal(mut self, value: Option<Bytes>) -> Self {
        self.request_principal = value;
        self
    }
    /// Sets `client_host_address` to the passed value.
    ///
    /// The original client's address in bytes.
    ///
    /// Supported API versions: 0
    pub fn with_client_host_address(mut self, value: Bytes) -> Self {
        self.client_host_address = value;
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
impl Encodable for EnvelopeRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactBytes.encode(buf, &self.request_data)?;
        types::CompactBytes.encode(buf, &self.request_principal)?;
        types::CompactBytes.encode(buf, &self.client_host_address)?;
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
        total_size += types::CompactBytes.compute_size(&self.request_data)?;
        total_size += types::CompactBytes.compute_size(&self.request_principal)?;
        total_size += types::CompactBytes.compute_size(&self.client_host_address)?;
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
impl Decodable for EnvelopeRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let request_data = types::CompactBytes.decode(buf)?;
        let request_principal = types::CompactBytes.decode(buf)?;
        let client_host_address = types::CompactBytes.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
            unknown_tagged_fields,
        })
    }
}

impl Default for EnvelopeRequest {
    fn default() -> Self {
        Self {
            request_data: Default::default(),
            request_principal: Some(Default::default()),
            client_host_address: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for EnvelopeRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for EnvelopeRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
