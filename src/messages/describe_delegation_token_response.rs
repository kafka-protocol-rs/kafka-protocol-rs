//! DescribeDelegationTokenResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeDelegationTokenResponse.json).
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
pub struct DescribeDelegationTokenResponse {
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-3
    pub error_code: i16,

    /// The tokens.
    ///
    /// Supported API versions: 1-3
    pub tokens: Vec<DescribedDelegationToken>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-3
    pub throttle_time_ms: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeDelegationTokenResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 1-3
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `tokens` to the passed value.
    ///
    /// The tokens.
    ///
    /// Supported API versions: 1-3
    pub fn with_tokens(mut self, value: Vec<DescribedDelegationToken>) -> Self {
        self.tokens = value;
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
impl Encodable for DescribeDelegationTokenResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 2 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.tokens)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.tokens)?;
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
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.tokens)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.tokens)?;
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
impl Decodable for DescribeDelegationTokenResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let tokens = if version >= 2 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
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
            tokens,
            throttle_time_ms,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeDelegationTokenResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            tokens: Default::default(),
            throttle_time_ms: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeDelegationTokenResponse {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedDelegationToken {
    /// The token principal type.
    ///
    /// Supported API versions: 1-3
    pub principal_type: StrBytes,

    /// The token principal name.
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

    /// The token issue timestamp in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub issue_timestamp: i64,

    /// The token expiry timestamp in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub expiry_timestamp: i64,

    /// The token maximum timestamp length in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub max_timestamp: i64,

    /// The token ID.
    ///
    /// Supported API versions: 1-3
    pub token_id: StrBytes,

    /// The token HMAC.
    ///
    /// Supported API versions: 1-3
    pub hmac: Bytes,

    /// Those who are able to renew this token before it expires.
    ///
    /// Supported API versions: 1-3
    pub renewers: Vec<DescribedDelegationTokenRenewer>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribedDelegationToken {
    /// Sets `principal_type` to the passed value.
    ///
    /// The token principal type.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_type(mut self, value: StrBytes) -> Self {
        self.principal_type = value;
        self
    }
    /// Sets `principal_name` to the passed value.
    ///
    /// The token principal name.
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
    /// Sets `issue_timestamp` to the passed value.
    ///
    /// The token issue timestamp in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub fn with_issue_timestamp(mut self, value: i64) -> Self {
        self.issue_timestamp = value;
        self
    }
    /// Sets `expiry_timestamp` to the passed value.
    ///
    /// The token expiry timestamp in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub fn with_expiry_timestamp(mut self, value: i64) -> Self {
        self.expiry_timestamp = value;
        self
    }
    /// Sets `max_timestamp` to the passed value.
    ///
    /// The token maximum timestamp length in milliseconds.
    ///
    /// Supported API versions: 1-3
    pub fn with_max_timestamp(mut self, value: i64) -> Self {
        self.max_timestamp = value;
        self
    }
    /// Sets `token_id` to the passed value.
    ///
    /// The token ID.
    ///
    /// Supported API versions: 1-3
    pub fn with_token_id(mut self, value: StrBytes) -> Self {
        self.token_id = value;
        self
    }
    /// Sets `hmac` to the passed value.
    ///
    /// The token HMAC.
    ///
    /// Supported API versions: 1-3
    pub fn with_hmac(mut self, value: Bytes) -> Self {
        self.hmac = value;
        self
    }
    /// Sets `renewers` to the passed value.
    ///
    /// Those who are able to renew this token before it expires.
    ///
    /// Supported API versions: 1-3
    pub fn with_renewers(mut self, value: Vec<DescribedDelegationTokenRenewer>) -> Self {
        self.renewers = value;
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
impl Encodable for DescribedDelegationToken {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
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
        types::Int64.encode(buf, &self.issue_timestamp)?;
        types::Int64.encode(buf, &self.expiry_timestamp)?;
        types::Int64.encode(buf, &self.max_timestamp)?;
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
        if version >= 2 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.renewers)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.renewers)?;
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
        total_size += types::Int64.compute_size(&self.issue_timestamp)?;
        total_size += types::Int64.compute_size(&self.expiry_timestamp)?;
        total_size += types::Int64.compute_size(&self.max_timestamp)?;
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
        if version >= 2 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.renewers)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.renewers)?;
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
impl Decodable for DescribedDelegationToken {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
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
        let issue_timestamp = types::Int64.decode(buf)?;
        let expiry_timestamp = types::Int64.decode(buf)?;
        let max_timestamp = types::Int64.decode(buf)?;
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
        let renewers = if version >= 2 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
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
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id,
            hmac,
            renewers,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribedDelegationToken {
    fn default() -> Self {
        Self {
            principal_type: Default::default(),
            principal_name: Default::default(),
            token_requester_principal_type: Default::default(),
            token_requester_principal_name: Default::default(),
            issue_timestamp: 0,
            expiry_timestamp: 0,
            max_timestamp: 0,
            token_id: Default::default(),
            hmac: Default::default(),
            renewers: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribedDelegationToken {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1-3
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedDelegationTokenRenewer {
    /// The renewer principal type.
    ///
    /// Supported API versions: 1-3
    pub principal_type: StrBytes,

    /// The renewer principal name.
    ///
    /// Supported API versions: 1-3
    pub principal_name: StrBytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribedDelegationTokenRenewer {
    /// Sets `principal_type` to the passed value.
    ///
    /// The renewer principal type.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_type(mut self, value: StrBytes) -> Self {
        self.principal_type = value;
        self
    }
    /// Sets `principal_name` to the passed value.
    ///
    /// The renewer principal name.
    ///
    /// Supported API versions: 1-3
    pub fn with_principal_name(mut self, value: StrBytes) -> Self {
        self.principal_name = value;
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
impl Encodable for DescribedDelegationTokenRenewer {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
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
impl Decodable for DescribedDelegationTokenRenewer {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 1 || version > 3 {
            bail!("specified version not supported by this message type");
        }
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
            principal_type,
            principal_name,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribedDelegationTokenRenewer {
    fn default() -> Self {
        Self {
            principal_type: Default::default(),
            principal_name: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribedDelegationTokenRenewer {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 3 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeDelegationTokenResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
