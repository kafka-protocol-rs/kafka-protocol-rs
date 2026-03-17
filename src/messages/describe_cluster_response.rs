//! DescribeClusterResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeClusterResponse.json).
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

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClusterBroker {
    /// The broker ID.
    ///
    /// Supported API versions: 0-2
    pub broker_id: super::BrokerId,

    /// The broker hostname.
    ///
    /// Supported API versions: 0-2
    pub host: StrBytes,

    /// The broker port.
    ///
    /// Supported API versions: 0-2
    pub port: i32,

    /// The rack of the broker, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 0-2
    pub rack: Option<StrBytes>,

    /// Whether the broker is fenced
    ///
    /// Supported API versions: 2
    pub is_fenced: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeClusterBroker {
    /// Sets `broker_id` to the passed value.
    ///
    /// The broker ID.
    ///
    /// Supported API versions: 0-2
    pub fn with_broker_id(mut self, value: super::BrokerId) -> Self {
        self.broker_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The broker hostname.
    ///
    /// Supported API versions: 0-2
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The broker port.
    ///
    /// Supported API versions: 0-2
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack of the broker, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 0-2
    pub fn with_rack(mut self, value: Option<StrBytes>) -> Self {
        self.rack = value;
        self
    }
    /// Sets `is_fenced` to the passed value.
    ///
    /// Whether the broker is fenced
    ///
    /// Supported API versions: 2
    pub fn with_is_fenced(mut self, value: bool) -> Self {
        self.is_fenced = value;
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
impl Encodable for DescribeClusterBroker {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.broker_id)?;
        types::CompactString.encode(buf, &self.host)?;
        types::Int32.encode(buf, &self.port)?;
        types::CompactString.encode(buf, &self.rack)?;
        if version >= 2 {
            types::Boolean.encode(buf, &self.is_fenced)?;
        } else {
            if self.is_fenced {
                bail!("A field is set that is not available on the selected protocol version");
            }
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
        total_size += types::CompactString.compute_size(&self.host)?;
        total_size += types::Int32.compute_size(&self.port)?;
        total_size += types::CompactString.compute_size(&self.rack)?;
        if version >= 2 {
            total_size += types::Boolean.compute_size(&self.is_fenced)?;
        } else {
            if self.is_fenced {
                bail!("A field is set that is not available on the selected protocol version");
            }
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

#[cfg(feature = "client")]
impl Decodable for DescribeClusterBroker {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let broker_id = types::Int32.decode(buf)?;
        let host = types::CompactString.decode(buf)?;
        let port = types::Int32.decode(buf)?;
        let rack = types::CompactString.decode(buf)?;
        let is_fenced = if version >= 2 {
            types::Boolean.decode(buf)?
        } else {
            false
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
            host,
            port,
            rack,
            is_fenced,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeClusterBroker {
    fn default() -> Self {
        Self {
            broker_id: (0).into(),
            host: Default::default(),
            port: 0,
            rack: None,
            is_fenced: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeClusterBroker {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClusterResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-2
    pub throttle_time_ms: i32,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-2
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0-2
    pub error_message: Option<StrBytes>,

    /// The endpoint type that was described. 1=brokers, 2=controllers.
    ///
    /// Supported API versions: 1-2
    pub endpoint_type: i8,

    /// The cluster ID that responding broker belongs to.
    ///
    /// Supported API versions: 0-2
    pub cluster_id: StrBytes,

    /// The ID of the controller broker.
    ///
    /// Supported API versions: 0-2
    pub controller_id: super::BrokerId,

    /// Each broker in the response.
    ///
    /// Supported API versions: 0-2
    pub brokers: Vec<DescribeClusterBroker>,

    /// 32-bit bitfield to represent authorized operations for this cluster.
    ///
    /// Supported API versions: 0-2
    pub cluster_authorized_operations: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeClusterResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-2
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0-2
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `endpoint_type` to the passed value.
    ///
    /// The endpoint type that was described. 1=brokers, 2=controllers.
    ///
    /// Supported API versions: 1-2
    pub fn with_endpoint_type(mut self, value: i8) -> Self {
        self.endpoint_type = value;
        self
    }
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster ID that responding broker belongs to.
    ///
    /// Supported API versions: 0-2
    pub fn with_cluster_id(mut self, value: StrBytes) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `controller_id` to the passed value.
    ///
    /// The ID of the controller broker.
    ///
    /// Supported API versions: 0-2
    pub fn with_controller_id(mut self, value: super::BrokerId) -> Self {
        self.controller_id = value;
        self
    }
    /// Sets `brokers` to the passed value.
    ///
    /// Each broker in the response.
    ///
    /// Supported API versions: 0-2
    pub fn with_brokers(mut self, value: Vec<DescribeClusterBroker>) -> Self {
        self.brokers = value;
        self
    }
    /// Sets `cluster_authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this cluster.
    ///
    /// Supported API versions: 0-2
    pub fn with_cluster_authorized_operations(mut self, value: i32) -> Self {
        self.cluster_authorized_operations = value;
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
impl Encodable for DescribeClusterResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        if version >= 1 {
            types::Int8.encode(buf, &self.endpoint_type)?;
        } else {
            if self.endpoint_type != 1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        types::CompactString.encode(buf, &self.cluster_id)?;
        types::Int32.encode(buf, &self.controller_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.brokers)?;
        types::Int32.encode(buf, &self.cluster_authorized_operations)?;
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
        total_size += types::CompactString.compute_size(&self.error_message)?;
        if version >= 1 {
            total_size += types::Int8.compute_size(&self.endpoint_type)?;
        } else {
            if self.endpoint_type != 1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        total_size += types::CompactString.compute_size(&self.cluster_id)?;
        total_size += types::Int32.compute_size(&self.controller_id)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.brokers)?;
        total_size += types::Int32.compute_size(&self.cluster_authorized_operations)?;
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
impl Decodable for DescribeClusterResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let endpoint_type = if version >= 1 {
            types::Int8.decode(buf)?
        } else {
            1
        };
        let cluster_id = types::CompactString.decode(buf)?;
        let controller_id = types::Int32.decode(buf)?;
        let brokers = types::CompactArray(types::Struct { version }).decode(buf)?;
        let cluster_authorized_operations = types::Int32.decode(buf)?;
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
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeClusterResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            endpoint_type: 1,
            cluster_id: Default::default(),
            controller_id: (-1).into(),
            brokers: Default::default(),
            cluster_authorized_operations: -2147483648,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeClusterResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeClusterResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
