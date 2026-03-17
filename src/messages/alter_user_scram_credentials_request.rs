//! AlterUserScramCredentialsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/AlterUserScramCredentialsRequest.json).
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
pub struct AlterUserScramCredentialsRequest {
    /// The SCRAM credentials to remove.
    ///
    /// Supported API versions: 0
    pub deletions: Vec<ScramCredentialDeletion>,

    /// The SCRAM credentials to update/insert.
    ///
    /// Supported API versions: 0
    pub upsertions: Vec<ScramCredentialUpsertion>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AlterUserScramCredentialsRequest {
    /// Sets `deletions` to the passed value.
    ///
    /// The SCRAM credentials to remove.
    ///
    /// Supported API versions: 0
    pub fn with_deletions(mut self, value: Vec<ScramCredentialDeletion>) -> Self {
        self.deletions = value;
        self
    }
    /// Sets `upsertions` to the passed value.
    ///
    /// The SCRAM credentials to update/insert.
    ///
    /// Supported API versions: 0
    pub fn with_upsertions(mut self, value: Vec<ScramCredentialUpsertion>) -> Self {
        self.upsertions = value;
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
impl Encodable for AlterUserScramCredentialsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.deletions)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.upsertions)?;
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
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.deletions)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.upsertions)?;
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
impl Decodable for AlterUserScramCredentialsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let deletions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let upsertions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            deletions,
            upsertions,
            unknown_tagged_fields,
        })
    }
}

impl Default for AlterUserScramCredentialsRequest {
    fn default() -> Self {
        Self {
            deletions: Default::default(),
            upsertions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AlterUserScramCredentialsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ScramCredentialDeletion {
    /// The user name.
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// The SCRAM mechanism.
    ///
    /// Supported API versions: 0
    pub mechanism: i8,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ScramCredentialDeletion {
    /// Sets `name` to the passed value.
    ///
    /// The user name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `mechanism` to the passed value.
    ///
    /// The SCRAM mechanism.
    ///
    /// Supported API versions: 0
    pub fn with_mechanism(mut self, value: i8) -> Self {
        self.mechanism = value;
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
impl Encodable for ScramCredentialDeletion {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::Int8.encode(buf, &self.mechanism)?;
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
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Int8.compute_size(&self.mechanism)?;
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
impl Decodable for ScramCredentialDeletion {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let mechanism = types::Int8.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            name,
            mechanism,
            unknown_tagged_fields,
        })
    }
}

impl Default for ScramCredentialDeletion {
    fn default() -> Self {
        Self {
            name: Default::default(),
            mechanism: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ScramCredentialDeletion {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ScramCredentialUpsertion {
    /// The user name.
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// The SCRAM mechanism.
    ///
    /// Supported API versions: 0
    pub mechanism: i8,

    /// The number of iterations.
    ///
    /// Supported API versions: 0
    pub iterations: i32,

    /// A random salt generated by the client.
    ///
    /// Supported API versions: 0
    pub salt: Bytes,

    /// The salted password.
    ///
    /// Supported API versions: 0
    pub salted_password: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ScramCredentialUpsertion {
    /// Sets `name` to the passed value.
    ///
    /// The user name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `mechanism` to the passed value.
    ///
    /// The SCRAM mechanism.
    ///
    /// Supported API versions: 0
    pub fn with_mechanism(mut self, value: i8) -> Self {
        self.mechanism = value;
        self
    }
    /// Sets `iterations` to the passed value.
    ///
    /// The number of iterations.
    ///
    /// Supported API versions: 0
    pub fn with_iterations(mut self, value: i32) -> Self {
        self.iterations = value;
        self
    }
    /// Sets `salt` to the passed value.
    ///
    /// A random salt generated by the client.
    ///
    /// Supported API versions: 0
    pub fn with_salt(mut self, value: Bytes) -> Self {
        self.salt = value;
        self
    }
    /// Sets `salted_password` to the passed value.
    ///
    /// The salted password.
    ///
    /// Supported API versions: 0
    pub fn with_salted_password(mut self, value: Bytes) -> Self {
        self.salted_password = value;
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
impl Encodable for ScramCredentialUpsertion {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::Int8.encode(buf, &self.mechanism)?;
        types::Int32.encode(buf, &self.iterations)?;
        types::CompactBytes.encode(buf, &self.salt)?;
        types::CompactBytes.encode(buf, &self.salted_password)?;
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
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Int8.compute_size(&self.mechanism)?;
        total_size += types::Int32.compute_size(&self.iterations)?;
        total_size += types::CompactBytes.compute_size(&self.salt)?;
        total_size += types::CompactBytes.compute_size(&self.salted_password)?;
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
impl Decodable for ScramCredentialUpsertion {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let mechanism = types::Int8.decode(buf)?;
        let iterations = types::Int32.decode(buf)?;
        let salt = types::CompactBytes.decode(buf)?;
        let salted_password = types::CompactBytes.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            name,
            mechanism,
            iterations,
            salt,
            salted_password,
            unknown_tagged_fields,
        })
    }
}

impl Default for ScramCredentialUpsertion {
    fn default() -> Self {
        Self {
            name: Default::default(),
            mechanism: 0,
            iterations: 0,
            salt: Default::default(),
            salted_password: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ScramCredentialUpsertion {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for AlterUserScramCredentialsRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
