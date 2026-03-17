//! BrokerRegistrationRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/BrokerRegistrationRequest.json).
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

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerRegistrationRequest {
    /// The broker ID.
    ///
    /// Supported API versions: 0-4
    pub broker_id: super::BrokerId,

    /// The cluster id of the broker process.
    ///
    /// Supported API versions: 0-4
    pub cluster_id: StrBytes,

    /// The incarnation id of the broker process.
    ///
    /// Supported API versions: 0-4
    pub incarnation_id: Uuid,

    /// The listeners of this broker.
    ///
    /// Supported API versions: 0-4
    pub listeners: Vec<Listener>,

    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    ///
    /// Supported API versions: 0-4
    pub features: Vec<Feature>,

    /// The rack which this broker is in.
    ///
    /// Supported API versions: 0-4
    pub rack: Option<StrBytes>,

    /// If the required configurations for ZK migration are present, this value is set to true.
    ///
    /// Supported API versions: 1-4
    pub is_migrating_zk_broker: bool,

    /// Log directories configured in this broker which are available.
    ///
    /// Supported API versions: 2-4
    pub log_dirs: Vec<Uuid>,

    /// The epoch before a clean shutdown.
    ///
    /// Supported API versions: 3-4
    pub previous_broker_epoch: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl BrokerRegistrationRequest {
    /// Sets `broker_id` to the passed value.
    ///
    /// The broker ID.
    ///
    /// Supported API versions: 0-4
    pub fn with_broker_id(mut self, value: super::BrokerId) -> Self {
        self.broker_id = value;
        self
    }
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster id of the broker process.
    ///
    /// Supported API versions: 0-4
    pub fn with_cluster_id(mut self, value: StrBytes) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `incarnation_id` to the passed value.
    ///
    /// The incarnation id of the broker process.
    ///
    /// Supported API versions: 0-4
    pub fn with_incarnation_id(mut self, value: Uuid) -> Self {
        self.incarnation_id = value;
        self
    }
    /// Sets `listeners` to the passed value.
    ///
    /// The listeners of this broker.
    ///
    /// Supported API versions: 0-4
    pub fn with_listeners(mut self, value: Vec<Listener>) -> Self {
        self.listeners = value;
        self
    }
    /// Sets `features` to the passed value.
    ///
    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    ///
    /// Supported API versions: 0-4
    pub fn with_features(mut self, value: Vec<Feature>) -> Self {
        self.features = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack which this broker is in.
    ///
    /// Supported API versions: 0-4
    pub fn with_rack(mut self, value: Option<StrBytes>) -> Self {
        self.rack = value;
        self
    }
    /// Sets `is_migrating_zk_broker` to the passed value.
    ///
    /// If the required configurations for ZK migration are present, this value is set to true.
    ///
    /// Supported API versions: 1-4
    pub fn with_is_migrating_zk_broker(mut self, value: bool) -> Self {
        self.is_migrating_zk_broker = value;
        self
    }
    /// Sets `log_dirs` to the passed value.
    ///
    /// Log directories configured in this broker which are available.
    ///
    /// Supported API versions: 2-4
    pub fn with_log_dirs(mut self, value: Vec<Uuid>) -> Self {
        self.log_dirs = value;
        self
    }
    /// Sets `previous_broker_epoch` to the passed value.
    ///
    /// The epoch before a clean shutdown.
    ///
    /// Supported API versions: 3-4
    pub fn with_previous_broker_epoch(mut self, value: i64) -> Self {
        self.previous_broker_epoch = value;
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
impl Encodable for BrokerRegistrationRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.broker_id)?;
        types::CompactString.encode(buf, &self.cluster_id)?;
        types::Uuid.encode(buf, &self.incarnation_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.listeners)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.features)?;
        types::CompactString.encode(buf, &self.rack)?;
        if version >= 1 {
            types::Boolean.encode(buf, &self.is_migrating_zk_broker)?;
        } else {
            if self.is_migrating_zk_broker {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            types::CompactArray(types::Uuid).encode(buf, &self.log_dirs)?;
        }
        if version >= 3 {
            types::Int64.encode(buf, &self.previous_broker_epoch)?;
        }
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
        total_size += types::Int32.compute_size(&self.broker_id)?;
        total_size += types::CompactString.compute_size(&self.cluster_id)?;
        total_size += types::Uuid.compute_size(&self.incarnation_id)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.listeners)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.features)?;
        total_size += types::CompactString.compute_size(&self.rack)?;
        if version >= 1 {
            total_size += types::Boolean.compute_size(&self.is_migrating_zk_broker)?;
        } else {
            if self.is_migrating_zk_broker {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            total_size += types::CompactArray(types::Uuid).compute_size(&self.log_dirs)?;
        }
        if version >= 3 {
            total_size += types::Int64.compute_size(&self.previous_broker_epoch)?;
        }
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
impl Decodable for BrokerRegistrationRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let broker_id = types::Int32.decode(buf)?;
        let cluster_id = types::CompactString.decode(buf)?;
        let incarnation_id = types::Uuid.decode(buf)?;
        let listeners = types::CompactArray(types::Struct { version }).decode(buf)?;
        let features = types::CompactArray(types::Struct { version }).decode(buf)?;
        let rack = types::CompactString.decode(buf)?;
        let is_migrating_zk_broker = if version >= 1 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let log_dirs = if version >= 2 {
            types::CompactArray(types::Uuid).decode(buf)?
        } else {
            Default::default()
        };
        let previous_broker_epoch = if version >= 3 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            broker_id,
            cluster_id,
            incarnation_id,
            listeners,
            features,
            rack,
            is_migrating_zk_broker,
            log_dirs,
            previous_broker_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for BrokerRegistrationRequest {
    fn default() -> Self {
        Self {
            broker_id: (0).into(),
            cluster_id: Default::default(),
            incarnation_id: Uuid::nil(),
            listeners: Default::default(),
            features: Default::default(),
            rack: Some(Default::default()),
            is_migrating_zk_broker: false,
            log_dirs: Default::default(),
            previous_broker_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for BrokerRegistrationRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    /// The feature name.
    ///
    /// Supported API versions: 0-4
    pub name: StrBytes,

    /// The minimum supported feature level.
    ///
    /// Supported API versions: 0-4
    pub min_supported_version: i16,

    /// The maximum supported feature level.
    ///
    /// Supported API versions: 0-4
    pub max_supported_version: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Feature {
    /// Sets `name` to the passed value.
    ///
    /// The feature name.
    ///
    /// Supported API versions: 0-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `min_supported_version` to the passed value.
    ///
    /// The minimum supported feature level.
    ///
    /// Supported API versions: 0-4
    pub fn with_min_supported_version(mut self, value: i16) -> Self {
        self.min_supported_version = value;
        self
    }
    /// Sets `max_supported_version` to the passed value.
    ///
    /// The maximum supported feature level.
    ///
    /// Supported API versions: 0-4
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
        if version < 0 || version > 4 {
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
        if version < 0 || version > 4 {
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
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    ///
    /// Supported API versions: 0-4
    pub name: StrBytes,

    /// The hostname.
    ///
    /// Supported API versions: 0-4
    pub host: StrBytes,

    /// The port.
    ///
    /// Supported API versions: 0-4
    pub port: u16,

    /// The security protocol.
    ///
    /// Supported API versions: 0-4
    pub security_protocol: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Listener {
    /// Sets `name` to the passed value.
    ///
    /// The name of the endpoint.
    ///
    /// Supported API versions: 0-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The hostname.
    ///
    /// Supported API versions: 0-4
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The port.
    ///
    /// Supported API versions: 0-4
    pub fn with_port(mut self, value: u16) -> Self {
        self.port = value;
        self
    }
    /// Sets `security_protocol` to the passed value.
    ///
    /// The security protocol.
    ///
    /// Supported API versions: 0-4
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
        if version < 0 || version > 4 {
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
        if version < 0 || version > 4 {
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
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for BrokerRegistrationRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
