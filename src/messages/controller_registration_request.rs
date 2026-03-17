//! ControllerRegistrationRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ControllerRegistrationRequest.json).
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
pub struct ControllerRegistrationRequest {
    /// The ID of the controller to register.
    ///
    /// Supported API versions: 0
    pub controller_id: i32,

    /// The controller incarnation ID, which is unique to each process run.
    ///
    /// Supported API versions: 0
    pub incarnation_id: Uuid,

    /// Set if the required configurations for ZK migration are present.
    ///
    /// Supported API versions: 0
    pub zk_migration_ready: bool,

    /// The listeners of this controller.
    ///
    /// Supported API versions: 0
    pub listeners: Vec<Listener>,

    /// The features on this controller.
    ///
    /// Supported API versions: 0
    pub features: Vec<Feature>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ControllerRegistrationRequest {
    /// Sets `controller_id` to the passed value.
    ///
    /// The ID of the controller to register.
    ///
    /// Supported API versions: 0
    pub fn with_controller_id(mut self, value: i32) -> Self {
        self.controller_id = value;
        self
    }
    /// Sets `incarnation_id` to the passed value.
    ///
    /// The controller incarnation ID, which is unique to each process run.
    ///
    /// Supported API versions: 0
    pub fn with_incarnation_id(mut self, value: Uuid) -> Self {
        self.incarnation_id = value;
        self
    }
    /// Sets `zk_migration_ready` to the passed value.
    ///
    /// Set if the required configurations for ZK migration are present.
    ///
    /// Supported API versions: 0
    pub fn with_zk_migration_ready(mut self, value: bool) -> Self {
        self.zk_migration_ready = value;
        self
    }
    /// Sets `listeners` to the passed value.
    ///
    /// The listeners of this controller.
    ///
    /// Supported API versions: 0
    pub fn with_listeners(mut self, value: Vec<Listener>) -> Self {
        self.listeners = value;
        self
    }
    /// Sets `features` to the passed value.
    ///
    /// The features on this controller.
    ///
    /// Supported API versions: 0
    pub fn with_features(mut self, value: Vec<Feature>) -> Self {
        self.features = value;
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
impl Encodable for ControllerRegistrationRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.controller_id)?;
        types::Uuid.encode(buf, &self.incarnation_id)?;
        types::Boolean.encode(buf, &self.zk_migration_ready)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.listeners)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.features)?;
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
        total_size += types::Int32.compute_size(&self.controller_id)?;
        total_size += types::Uuid.compute_size(&self.incarnation_id)?;
        total_size += types::Boolean.compute_size(&self.zk_migration_ready)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.listeners)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.features)?;
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
impl Decodable for ControllerRegistrationRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let controller_id = types::Int32.decode(buf)?;
        let incarnation_id = types::Uuid.decode(buf)?;
        let zk_migration_ready = types::Boolean.decode(buf)?;
        let listeners = types::CompactArray(types::Struct { version }).decode(buf)?;
        let features = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            controller_id,
            incarnation_id,
            zk_migration_ready,
            listeners,
            features,
            unknown_tagged_fields,
        })
    }
}

impl Default for ControllerRegistrationRequest {
    fn default() -> Self {
        Self {
            controller_id: 0,
            incarnation_id: Uuid::nil(),
            zk_migration_ready: false,
            listeners: Default::default(),
            features: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ControllerRegistrationRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    /// The feature name.
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// The minimum supported feature level.
    ///
    /// Supported API versions: 0
    pub min_supported_version: i16,

    /// The maximum supported feature level.
    ///
    /// Supported API versions: 0
    pub max_supported_version: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Feature {
    /// Sets `name` to the passed value.
    ///
    /// The feature name.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `min_supported_version` to the passed value.
    ///
    /// The minimum supported feature level.
    ///
    /// Supported API versions: 0
    pub fn with_min_supported_version(mut self, value: i16) -> Self {
        self.min_supported_version = value;
        self
    }
    /// Sets `max_supported_version` to the passed value.
    ///
    /// The maximum supported feature level.
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

#[cfg(feature = "client")]
impl Encodable for Feature {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
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
        total_size += types::CompactString.compute_size(&self.name)?;
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

#[cfg(feature = "broker")]
impl Decodable for Feature {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
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
            name,
            min_supported_version,
            max_supported_version,
            unknown_tagged_fields,
        })
    }
}

impl Default for Feature {
    fn default() -> Self {
        Self {
            name: Default::default(),
            min_supported_version: 0,
            max_supported_version: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Feature {
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

    /// The security protocol.
    ///
    /// Supported API versions: 0
    pub security_protocol: i16,

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
    /// Sets `security_protocol` to the passed value.
    ///
    /// The security protocol.
    ///
    /// Supported API versions: 0
    pub fn with_security_protocol(mut self, value: i16) -> Self {
        self.security_protocol = value;
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
        types::Int16.encode(buf, &self.security_protocol)?;
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
        total_size += types::Int16.compute_size(&self.security_protocol)?;
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
        let security_protocol = types::Int16.decode(buf)?;
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
            security_protocol,
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
            security_protocol: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Listener {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ControllerRegistrationRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
