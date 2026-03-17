//! RequestHeader
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/RequestHeader.json).
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

/// Valid versions: 1-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct RequestHeader {
    /// The API key of this request.
    ///
    /// Supported API versions: 1-2
    pub request_api_key: i16,

    /// The API version of this request.
    ///
    /// Supported API versions: 1-2
    pub request_api_version: i16,

    /// The correlation ID of this request.
    ///
    /// Supported API versions: 1-2
    pub correlation_id: i32,

    /// The client ID string.
    ///
    /// Supported API versions: 1-2
    pub client_id: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl RequestHeader {
    /// Sets `request_api_key` to the passed value.
    ///
    /// The API key of this request.
    ///
    /// Supported API versions: 1-2
    pub fn with_request_api_key(mut self, value: i16) -> Self {
        self.request_api_key = value;
        self
    }
    /// Sets `request_api_version` to the passed value.
    ///
    /// The API version of this request.
    ///
    /// Supported API versions: 1-2
    pub fn with_request_api_version(mut self, value: i16) -> Self {
        self.request_api_version = value;
        self
    }
    /// Sets `correlation_id` to the passed value.
    ///
    /// The correlation ID of this request.
    ///
    /// Supported API versions: 1-2
    pub fn with_correlation_id(mut self, value: i32) -> Self {
        self.correlation_id = value;
        self
    }
    /// Sets `client_id` to the passed value.
    ///
    /// The client ID string.
    ///
    /// Supported API versions: 1-2
    pub fn with_client_id(mut self, value: Option<StrBytes>) -> Self {
        self.client_id = value;
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

impl Encodable for RequestHeader {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.request_api_key)?;
        types::Int16.encode(buf, &self.request_api_version)?;
        types::Int32.encode(buf, &self.correlation_id)?;
        types::String.encode(buf, &self.client_id)?;
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
        total_size += types::Int16.compute_size(&self.request_api_key)?;
        total_size += types::Int16.compute_size(&self.request_api_version)?;
        total_size += types::Int32.compute_size(&self.correlation_id)?;
        total_size += types::String.compute_size(&self.client_id)?;
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

impl Decodable for RequestHeader {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let request_api_key = types::Int16.decode(buf)?;
        let request_api_version = types::Int16.decode(buf)?;
        let correlation_id = types::Int32.decode(buf)?;
        let client_id = types::String.decode(buf)?;
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
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for RequestHeader {
    fn default() -> Self {
        Self {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 0,
            client_id: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for RequestHeader {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
