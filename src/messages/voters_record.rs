//! VotersRecord
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/VotersRecord.json).
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
pub struct Endpoint {
    /// The name of the endpoint.
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// The hostname.
    ///
    /// Supported API versions: 0
    pub host: StrBytes,

    /// The port.
    ///
    /// Supported API versions: 0
    pub port: u16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Endpoint {
    /// Sets `name` to the passed value.
    ///
    /// The name of the endpoint.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The hostname.
    ///
    /// Supported API versions: 0
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The port.
    ///
    /// Supported API versions: 0
    pub fn with_port(mut self, value: u16) -> Self {
        self.port = value;
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

impl Encodable for Endpoint {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::CompactString.encode(buf, &self.host)?;
        types::UInt16.encode(buf, &self.port)?;
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
        total_size += types::CompactString.compute_size(&self.host)?;
        total_size += types::UInt16.compute_size(&self.port)?;
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

impl Decodable for Endpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let host = types::CompactString.decode(buf)?;
        let port = types::UInt16.decode(buf)?;
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
            host,
            port,
            unknown_tagged_fields,
        })
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        Self {
            name: Default::default(),
            host: Default::default(),
            port: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Endpoint {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct KRaftVersionFeature {
    /// The minimum supported KRaft protocol version.
    ///
    /// Supported API versions: 0
    pub min_supported_version: i16,

    /// The maximum supported KRaft protocol version.
    ///
    /// Supported API versions: 0
    pub max_supported_version: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl KRaftVersionFeature {
    /// Sets `min_supported_version` to the passed value.
    ///
    /// The minimum supported KRaft protocol version.
    ///
    /// Supported API versions: 0
    pub fn with_min_supported_version(mut self, value: i16) -> Self {
        self.min_supported_version = value;
        self
    }
    /// Sets `max_supported_version` to the passed value.
    ///
    /// The maximum supported KRaft protocol version.
    ///
    /// Supported API versions: 0
    pub fn with_max_supported_version(mut self, value: i16) -> Self {
        self.max_supported_version = value;
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

impl Encodable for KRaftVersionFeature {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.min_supported_version)?;
        types::Int16.encode(buf, &self.max_supported_version)?;
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
        total_size += types::Int16.compute_size(&self.min_supported_version)?;
        total_size += types::Int16.compute_size(&self.max_supported_version)?;
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

impl Decodable for KRaftVersionFeature {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let min_supported_version = types::Int16.decode(buf)?;
        let max_supported_version = types::Int16.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
            unknown_tagged_fields,
        })
    }
}

impl Default for KRaftVersionFeature {
    fn default() -> Self {
        Self {
            min_supported_version: 0,
            max_supported_version: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for KRaftVersionFeature {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Voter {
    /// The replica id of the voter in the topic partition.
    ///
    /// Supported API versions: 0
    pub voter_id: super::BrokerId,

    /// The directory id of the voter in the topic partition.
    ///
    /// Supported API versions: 0
    pub voter_directory_id: Uuid,

    /// The endpoint that can be used to communicate with the voter.
    ///
    /// Supported API versions: 0
    pub endpoints: Vec<Endpoint>,

    /// The range of versions of the protocol that the replica supports.
    ///
    /// Supported API versions: 0
    pub k_raft_version_feature: KRaftVersionFeature,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Voter {
    /// Sets `voter_id` to the passed value.
    ///
    /// The replica id of the voter in the topic partition.
    ///
    /// Supported API versions: 0
    pub fn with_voter_id(mut self, value: super::BrokerId) -> Self {
        self.voter_id = value;
        self
    }
    /// Sets `voter_directory_id` to the passed value.
    ///
    /// The directory id of the voter in the topic partition.
    ///
    /// Supported API versions: 0
    pub fn with_voter_directory_id(mut self, value: Uuid) -> Self {
        self.voter_directory_id = value;
        self
    }
    /// Sets `endpoints` to the passed value.
    ///
    /// The endpoint that can be used to communicate with the voter.
    ///
    /// Supported API versions: 0
    pub fn with_endpoints(mut self, value: Vec<Endpoint>) -> Self {
        self.endpoints = value;
        self
    }
    /// Sets `k_raft_version_feature` to the passed value.
    ///
    /// The range of versions of the protocol that the replica supports.
    ///
    /// Supported API versions: 0
    pub fn with_k_raft_version_feature(mut self, value: KRaftVersionFeature) -> Self {
        self.k_raft_version_feature = value;
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

impl Encodable for Voter {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.voter_id)?;
        types::Uuid.encode(buf, &self.voter_directory_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.endpoints)?;
        types::Struct { version }.encode(buf, &self.k_raft_version_feature)?;
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
        total_size += types::Int32.compute_size(&self.voter_id)?;
        total_size += types::Uuid.compute_size(&self.voter_directory_id)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.endpoints)?;
        total_size += types::Struct { version }.compute_size(&self.k_raft_version_feature)?;
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

impl Decodable for Voter {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let voter_id = types::Int32.decode(buf)?;
        let voter_directory_id = types::Uuid.decode(buf)?;
        let endpoints = types::CompactArray(types::Struct { version }).decode(buf)?;
        let k_raft_version_feature = types::Struct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            voter_id,
            voter_directory_id,
            endpoints,
            k_raft_version_feature,
            unknown_tagged_fields,
        })
    }
}

impl Default for Voter {
    fn default() -> Self {
        Self {
            voter_id: (0).into(),
            voter_directory_id: Uuid::nil(),
            endpoints: Default::default(),
            k_raft_version_feature: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Voter {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct VotersRecord {
    /// The version of the voters record.
    ///
    /// Supported API versions: 0
    pub version: i16,

    /// The set of voters in the quorum for this epoch.
    ///
    /// Supported API versions: 0
    pub voters: Vec<Voter>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl VotersRecord {
    /// Sets `version` to the passed value.
    ///
    /// The version of the voters record.
    ///
    /// Supported API versions: 0
    pub fn with_version(mut self, value: i16) -> Self {
        self.version = value;
        self
    }
    /// Sets `voters` to the passed value.
    ///
    /// The set of voters in the quorum for this epoch.
    ///
    /// Supported API versions: 0
    pub fn with_voters(mut self, value: Vec<Voter>) -> Self {
        self.voters = value;
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

impl Encodable for VotersRecord {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.version)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.voters)?;
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
        total_size += types::Int16.compute_size(&self.version)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.voters)?;
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

impl Decodable for VotersRecord {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let version = types::Int16.decode(buf)?;
        let voters = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            version,
            voters,
            unknown_tagged_fields,
        })
    }
}

impl Default for VotersRecord {
    fn default() -> Self {
        Self {
            version: 0,
            voters: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for VotersRecord {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
