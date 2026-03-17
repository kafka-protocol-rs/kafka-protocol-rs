//! ExpireDelegationTokenResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ExpireDelegationTokenResponse.json).
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
pub struct ExpireDelegationTokenResponse {
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-2
    pub error_code: i16,

    /// The timestamp in milliseconds at which this token expires.
    ///
    /// Supported API versions: 1-2
    pub expiry_timestamp_ms: i64,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-2
    pub throttle_time_ms: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ExpireDelegationTokenResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-2
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `expiry_timestamp_ms` to the passed value.
    ///
    /// The timestamp in milliseconds at which this token expires.
    ///
    /// Supported API versions: 1-2
    pub fn with_expiry_timestamp_ms(mut self, value: i64) -> Self {
        self.expiry_timestamp_ms = value;
        self
    }
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-2
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
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
impl Encodable for ExpireDelegationTokenResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::Int64.encode(buf, &self.expiry_timestamp_ms)?;
        types::Int32.encode(buf, &self.throttle_time_ms)?;
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
        total_size += types::Int64.compute_size(&self.expiry_timestamp_ms)?;
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
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
impl Decodable for ExpireDelegationTokenResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let expiry_timestamp_ms = types::Int64.decode(buf)?;
        let throttle_time_ms = types::Int32.decode(buf)?;
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
            expiry_timestamp_ms,
            throttle_time_ms,
            unknown_tagged_fields,
        })
    }
}

impl Default for ExpireDelegationTokenResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            expiry_timestamp_ms: 0,
            throttle_time_ms: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ExpireDelegationTokenResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ExpireDelegationTokenResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
