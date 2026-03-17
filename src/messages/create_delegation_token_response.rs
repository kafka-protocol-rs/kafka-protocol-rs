//! CreateDelegationTokenResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/CreateDelegationTokenResponse.json).
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

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDelegationTokenResponse {
    /// The top-level error, or zero if there was no error.
    ///
    /// Supported API versions: 1-3
    pub error_code: i16,

    /// The principal type of the token owner.
    ///
    /// Supported API versions: 1-3
    pub principal_type: StrBytes,

    /// The name of the token owner.
    ///
    /// Supported API versions: 1-3
    pub principal_name: StrBytes,

    /// The principal type of the requester of the token.
    ///
    /// Supported API versions: 3
    pub token_requester_principal_type: StrBytes,

    /// The principal type of the requester of the token.
    ///
    /// Supported API versions: 3
    pub token_requester_principal_name: StrBytes,

    /// When this token was generated.
    ///
    /// Supported API versions: 1-3
    pub issue_timestamp_ms: i64,

    /// When this token expires.
    ///
    /// Supported API versions: 1-3
    pub expiry_timestamp_ms: i64,

    /// The maximum lifetime of this token.
    ///
    /// Supported API versions: 1-3
    pub max_timestamp_ms: i64,

    /// The token UUID.
    ///
    /// Supported API versions: 1-3
    pub token_id: StrBytes,

    /// HMAC of the delegation token.
    ///
    /// Supported API versions: 1-3
    pub hmac: Bytes,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-3
    pub throttle_time_ms: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CreateDelegationTokenResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error, or zero if there was no error.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `principal_type` to the passed value.
    ///
    /// The principal type of the token owner.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_type(mut self, value: StrBytes) -> Self {
        self.principal_type = value;
        self
    }
    /// Sets `principal_name` to the passed value.
    ///
    /// The name of the token owner.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_name(mut self, value: StrBytes) -> Self {
        self.principal_name = value;
        self
    }
    /// Sets `token_requester_principal_type` to the passed value.
    ///
    /// The principal type of the requester of the token.
    ///
    /// Supported API versions: 3
    pub fn with_token_requester_principal_type(mut self, value: StrBytes) -> Self {
        self.token_requester_principal_type = value;
        self
    }
    /// Sets `token_requester_principal_name` to the passed value.
    ///
    /// The principal type of the requester of the token.
    ///
    /// Supported API versions: 3
    pub fn with_token_requester_principal_name(mut self, value: StrBytes) -> Self {
        self.token_requester_principal_name = value;
        self
    }
    /// Sets `issue_timestamp_ms` to the passed value.
    ///
    /// When this token was generated.
    ///
    /// Supported API versions: 1-3
    pub fn with_issue_timestamp_ms(mut self, value: i64) -> Self {
        self.issue_timestamp_ms = value;
        self
    }
    /// Sets `expiry_timestamp_ms` to the passed value.
    ///
    /// When this token expires.
    ///
    /// Supported API versions: 1-3
    pub fn with_expiry_timestamp_ms(mut self, value: i64) -> Self {
        self.expiry_timestamp_ms = value;
        self
    }
    /// Sets `max_timestamp_ms` to the passed value.
    ///
    /// The maximum lifetime of this token.
    ///
    /// Supported API versions: 1-3
    pub fn with_max_timestamp_ms(mut self, value: i64) -> Self {
        self.max_timestamp_ms = value;
        self
    }
    /// Sets `token_id` to the passed value.
    ///
    /// The token UUID.
    ///
    /// Supported API versions: 1-3
    pub fn with_token_id(mut self, value: StrBytes) -> Self {
        self.token_id = value;
        self
    }
    /// Sets `hmac` to the passed value.
    ///
    /// HMAC of the delegation token.
    ///
    /// Supported API versions: 1-3
    pub fn with_hmac(mut self, value: Bytes) -> Self {
        self.hmac = value;
        self
    }
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-3
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
impl Encodable for CreateDelegationTokenResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.principal_type)?;
        } else {
            types::String.encode(buf, &self.principal_type)?;
        }
        if version >= 2 {
            types::CompactString.encode(buf, &self.principal_name)?;
        } else {
            types::String.encode(buf, &self.principal_name)?;
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.token_requester_principal_type)?;
        } else {
            if !self.token_requester_principal_type.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.token_requester_principal_name)?;
        } else {
            if !self.token_requester_principal_name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        types::Int64.encode(buf, &self.issue_timestamp_ms)?;
        types::Int64.encode(buf, &self.expiry_timestamp_ms)?;
        types::Int64.encode(buf, &self.max_timestamp_ms)?;
        if version >= 2 {
            types::CompactString.encode(buf, &self.token_id)?;
        } else {
            types::String.encode(buf, &self.token_id)?;
        }
        if version >= 2 {
            types::CompactBytes.encode(buf, &self.hmac)?;
        } else {
            types::Bytes.encode(buf, &self.hmac)?;
        }
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
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.principal_type)?;
        } else {
            total_size += types::String.compute_size(&self.principal_type)?;
        }
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.principal_name)?;
        } else {
            total_size += types::String.compute_size(&self.principal_name)?;
        }
        if version >= 3 {
            total_size +=
                types::CompactString.compute_size(&self.token_requester_principal_type)?;
        } else {
            if !self.token_requester_principal_type.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size +=
                types::CompactString.compute_size(&self.token_requester_principal_name)?;
        } else {
            if !self.token_requester_principal_name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        total_size += types::Int64.compute_size(&self.issue_timestamp_ms)?;
        total_size += types::Int64.compute_size(&self.expiry_timestamp_ms)?;
        total_size += types::Int64.compute_size(&self.max_timestamp_ms)?;
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.token_id)?;
        } else {
            total_size += types::String.compute_size(&self.token_id)?;
        }
        if version >= 2 {
            total_size += types::CompactBytes.compute_size(&self.hmac)?;
        } else {
            total_size += types::Bytes.compute_size(&self.hmac)?;
        }
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
impl Decodable for CreateDelegationTokenResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let principal_type = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let principal_name = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let token_requester_principal_type = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let issue_timestamp_ms = types::Int64.decode(buf)?;
        let expiry_timestamp_ms = types::Int64.decode(buf)?;
        let max_timestamp_ms = types::Int64.decode(buf)?;
        let token_id = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let hmac = if version >= 2 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
        };
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
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
            unknown_tagged_fields,
        })
    }
}

impl Default for CreateDelegationTokenResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            principal_type: Default::default(),
            principal_name: Default::default(),
            token_requester_principal_type: Default::default(),
            token_requester_principal_name: Default::default(),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            max_timestamp_ms: 0,
            token_id: Default::default(),
            hmac: Default::default(),
            throttle_time_ms: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CreateDelegationTokenResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for CreateDelegationTokenResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
