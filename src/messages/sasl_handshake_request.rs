//! SaslHandshakeRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/SaslHandshakeRequest.json).
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
pub struct SaslHandshakeRequest {
    /// The SASL mechanism chosen by the client.
    ///
    /// Supported API versions: 0-1
    pub mechanism: StrBytes,
}

impl SaslHandshakeRequest {
    /// Sets `mechanism` to the passed value.
    ///
    /// The SASL mechanism chosen by the client.
    ///
    /// Supported API versions: 0-1
    pub fn with_mechanism(mut self, value: StrBytes) -> Self {
        self.mechanism = value;
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for SaslHandshakeRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::String.encode(buf, &self.mechanism)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::String.compute_size(&self.mechanism)?;

        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for SaslHandshakeRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let mechanism = types::String.decode(buf)?;
        Ok(Self { mechanism })
    }
}

impl Default for SaslHandshakeRequest {
    fn default() -> Self {
        Self {
            mechanism: Default::default(),
        }
    }
}

impl Message for SaslHandshakeRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for SaslHandshakeRequest {
    fn header_version(version: i16) -> i16 {
        1
    }
}
