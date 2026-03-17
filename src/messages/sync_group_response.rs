//! SyncGroupResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/SyncGroupResponse.json).
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

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-5
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-5
    pub error_code: i16,

    /// The group protocol type.
    ///
    /// Supported API versions: 5
    pub protocol_type: Option<StrBytes>,

    /// The group protocol name.
    ///
    /// Supported API versions: 5
    pub protocol_name: Option<StrBytes>,

    /// The member assignment.
    ///
    /// Supported API versions: 0-5
    pub assignment: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SyncGroupResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-5
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-5
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `protocol_type` to the passed value.
    ///
    /// The group protocol type.
    ///
    /// Supported API versions: 5
    pub fn with_protocol_type(mut self, value: Option<StrBytes>) -> Self {
        self.protocol_type = value;
        self
    }
    /// Sets `protocol_name` to the passed value.
    ///
    /// The group protocol name.
    ///
    /// Supported API versions: 5
    pub fn with_protocol_name(mut self, value: Option<StrBytes>) -> Self {
        self.protocol_name = value;
        self
    }
    /// Sets `assignment` to the passed value.
    ///
    /// The member assignment.
    ///
    /// Supported API versions: 0-5
    pub fn with_assignment(mut self, value: Bytes) -> Self {
        self.assignment = value;
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
impl Encodable for SyncGroupResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 5 {
            types::CompactString.encode(buf, &self.protocol_type)?;
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.protocol_name)?;
        }
        if version >= 4 {
            types::CompactBytes.encode(buf, &self.assignment)?;
        } else {
            types::Bytes.encode(buf, &self.assignment)?;
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
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.protocol_type)?;
        }
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.protocol_name)?;
        }
        if version >= 4 {
            total_size += types::CompactBytes.compute_size(&self.assignment)?;
        } else {
            total_size += types::Bytes.compute_size(&self.assignment)?;
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
impl Decodable for SyncGroupResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let error_code = types::Int16.decode(buf)?;
        let protocol_type = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let protocol_name = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let assignment = if version >= 4 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
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
            error_code,
            protocol_type,
            protocol_name,
            assignment,
            unknown_tagged_fields,
        })
    }
}

impl Default for SyncGroupResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            protocol_type: None,
            protocol_name: None,
            assignment: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SyncGroupResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for SyncGroupResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 4 {
            1
        } else {
            0
        }
    }
}
