//! AddRaftVoterRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/AddRaftVoterRequest.json).
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
pub struct AddRaftVoterRequest {
    /// The cluster id.
    ///
    /// Supported API versions: 0
    pub cluster_id: Option<StrBytes>,

    /// The maximum time to wait for the request to complete before returning.
    ///
    /// Supported API versions: 0
    pub timeout_ms: i32,

    /// The replica id of the voter getting added to the topic partition.
    ///
    /// Supported API versions: 0
    pub voter_id: i32,

    /// The directory id of the voter getting added to the topic partition.
    ///
    /// Supported API versions: 0
    pub voter_directory_id: Uuid,

    /// The endpoints that can be used to communicate with the voter.
    ///
    /// Supported API versions: 0
    pub listeners: Vec<Listener>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AddRaftVoterRequest {
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster id.
    ///
    /// Supported API versions: 0
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `timeout_ms` to the passed value.
    ///
    /// The maximum time to wait for the request to complete before returning.
    ///
    /// Supported API versions: 0
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    /// Sets `voter_id` to the passed value.
    ///
    /// The replica id of the voter getting added to the topic partition.
    ///
    /// Supported API versions: 0
    pub fn with_voter_id(mut self, value: i32) -> Self {
        self.voter_id = value;
        self
    }
    /// Sets `voter_directory_id` to the passed value.
    ///
    /// The directory id of the voter getting added to the topic partition.
    ///
    /// Supported API versions: 0
    pub fn with_voter_directory_id(mut self, value: Uuid) -> Self {
        self.voter_directory_id = value;
        self
    }
    /// Sets `listeners` to the passed value.
    ///
    /// The endpoints that can be used to communicate with the voter.
    ///
    /// Supported API versions: 0
    pub fn with_listeners(mut self, value: Vec<Listener>) -> Self {
        self.listeners = value;
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
impl Encodable for AddRaftVoterRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.cluster_id)?;
        types::Int32.encode(buf, &self.timeout_ms)?;
        types::Int32.encode(buf, &self.voter_id)?;
        types::Uuid.encode(buf, &self.voter_directory_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.listeners)?;
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
        total_size += types::CompactString.compute_size(&self.cluster_id)?;
        total_size += types::Int32.compute_size(&self.timeout_ms)?;
        total_size += types::Int32.compute_size(&self.voter_id)?;
        total_size += types::Uuid.compute_size(&self.voter_directory_id)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.listeners)?;
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
impl Decodable for AddRaftVoterRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let cluster_id = types::CompactString.decode(buf)?;
        let timeout_ms = types::Int32.decode(buf)?;
        let voter_id = types::Int32.decode(buf)?;
        let voter_directory_id = types::Uuid.decode(buf)?;
        let listeners = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            cluster_id,
            timeout_ms,
            voter_id,
            voter_directory_id,
            listeners,
            unknown_tagged_fields,
        })
    }
}

impl Default for AddRaftVoterRequest {
    fn default() -> Self {
        Self {
            cluster_id: Some(Default::default()),
            timeout_ms: 0,
            voter_id: 0,
            voter_directory_id: Uuid::nil(),
            listeners: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AddRaftVoterRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Listener {
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

impl Listener {
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

#[cfg(feature = "client")]
impl Encodable for Listener {
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

#[cfg(feature = "broker")]
impl Decodable for Listener {
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

impl Default for Listener {
    fn default() -> Self {
        Self {
            name: Default::default(),
            host: Default::default(),
            port: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Listener {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for AddRaftVoterRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
