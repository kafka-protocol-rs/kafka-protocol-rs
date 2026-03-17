//! SaslAuthenticateResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/SaslAuthenticateResponse.json).
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

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SaslAuthenticateResponse {
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-2
    pub error_code: i16,

    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 0-2
    pub error_message: Option<StrBytes>,

    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    ///
    /// Supported API versions: 0-2
    pub auth_bytes: Bytes,

    /// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    ///
    /// Supported API versions: 1-2
    pub session_lifetime_ms: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SaslAuthenticateResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The error message, or null if there was no error.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `auth_bytes` to the passed value.
    ///
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    ///
    /// Supported API versions: 0-2
    pub fn with_auth_bytes(mut self, value: Bytes) -> Self {
        self.auth_bytes = value;
        self
    }
    /// Sets `session_lifetime_ms` to the passed value.
    ///
    /// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    ///
    /// Supported API versions: 1-2
    pub fn with_session_lifetime_ms(mut self, value: i64) -> Self {
        self.session_lifetime_ms = value;
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
impl Encodable for SaslAuthenticateResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.error_message)?;
        } else {
            types::String.encode(buf, &self.error_message)?;
        }
        if version >= 2 {
            types::CompactBytes.encode(buf, &self.auth_bytes)?;
        } else {
            types::Bytes.encode(buf, &self.auth_bytes)?;
        }
        if version >= 1 {
            types::Int64.encode(buf, &self.session_lifetime_ms)?;
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
            total_size += types::CompactBytes.compute_size(&self.auth_bytes)?;
        } else {
            total_size += types::Bytes.compute_size(&self.auth_bytes)?;
        }
        if version >= 1 {
            total_size += types::Int64.compute_size(&self.session_lifetime_ms)?;
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
impl Decodable for SaslAuthenticateResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let auth_bytes = if version >= 2 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
        };
        let session_lifetime_ms = if version >= 1 {
            types::Int64.decode(buf)?
        } else {
            0
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
            auth_bytes,
            session_lifetime_ms,
            unknown_tagged_fields,
        })
    }
}

impl Default for SaslAuthenticateResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: Some(Default::default()),
            auth_bytes: Default::default(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SaslAuthenticateResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for SaslAuthenticateResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
