//! BrokerHeartbeatResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/BrokerHeartbeatResponse.json).
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
pub struct BrokerHeartbeatResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-1
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-1
    pub error_code: i16,

    /// True if the broker has approximately caught up with the latest metadata.
    ///
    /// Supported API versions: 0-1
    pub is_caught_up: bool,

    /// True if the broker is fenced.
    ///
    /// Supported API versions: 0-1
    pub is_fenced: bool,

    /// True if the broker should proceed with its shutdown.
    ///
    /// Supported API versions: 0-1
    pub should_shut_down: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl BrokerHeartbeatResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-1
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-1
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `is_caught_up` to the passed value.
    ///
    /// True if the broker has approximately caught up with the latest metadata.
    ///
    /// Supported API versions: 0-1
    pub fn with_is_caught_up(mut self, value: bool) -> Self {
        self.is_caught_up = value;
        self
    }
    /// Sets `is_fenced` to the passed value.
    ///
    /// True if the broker is fenced.
    ///
    /// Supported API versions: 0-1
    pub fn with_is_fenced(mut self, value: bool) -> Self {
        self.is_fenced = value;
        self
    }
    /// Sets `should_shut_down` to the passed value.
    ///
    /// True if the broker should proceed with its shutdown.
    ///
    /// Supported API versions: 0-1
    pub fn with_should_shut_down(mut self, value: bool) -> Self {
        self.should_shut_down = value;
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
impl Encodable for BrokerHeartbeatResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Boolean.encode(buf, &self.is_caught_up)?;
        types::Boolean.encode(buf, &self.is_fenced)?;
        types::Boolean.encode(buf, &self.should_shut_down)?;
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Boolean.compute_size(&self.is_caught_up)?;
        total_size += types::Boolean.compute_size(&self.is_fenced)?;
        total_size += types::Boolean.compute_size(&self.should_shut_down)?;
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

#[cfg(feature = "client")]
impl Decodable for BrokerHeartbeatResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let is_caught_up = types::Boolean.decode(buf)?;
        let is_fenced = types::Boolean.decode(buf)?;
        let should_shut_down = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            is_caught_up,
            is_fenced,
            should_shut_down,
            unknown_tagged_fields,
        })
    }
}

impl Default for BrokerHeartbeatResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for BrokerHeartbeatResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for BrokerHeartbeatResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
