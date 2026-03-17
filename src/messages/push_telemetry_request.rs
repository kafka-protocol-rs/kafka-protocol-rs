//! PushTelemetryRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/PushTelemetryRequest.json).
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
pub struct PushTelemetryRequest {
    /// Unique id for this client instance.
    ///
    /// Supported API versions: 0
    pub client_instance_id: Uuid,

    /// Unique identifier for the current subscription.
    ///
    /// Supported API versions: 0
    pub subscription_id: i32,

    /// Client is terminating the connection.
    ///
    /// Supported API versions: 0
    pub terminating: bool,

    /// Compression codec used to compress the metrics.
    ///
    /// Supported API versions: 0
    pub compression_type: i8,

    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    ///
    /// Supported API versions: 0
    pub metrics: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PushTelemetryRequest {
    /// Sets `client_instance_id` to the passed value.
    ///
    /// Unique id for this client instance.
    ///
    /// Supported API versions: 0
    pub fn with_client_instance_id(mut self, value: Uuid) -> Self {
        self.client_instance_id = value;
        self
    }
    /// Sets `subscription_id` to the passed value.
    ///
    /// Unique identifier for the current subscription.
    ///
    /// Supported API versions: 0
    pub fn with_subscription_id(mut self, value: i32) -> Self {
        self.subscription_id = value;
        self
    }
    /// Sets `terminating` to the passed value.
    ///
    /// Client is terminating the connection.
    ///
    /// Supported API versions: 0
    pub fn with_terminating(mut self, value: bool) -> Self {
        self.terminating = value;
        self
    }
    /// Sets `compression_type` to the passed value.
    ///
    /// Compression codec used to compress the metrics.
    ///
    /// Supported API versions: 0
    pub fn with_compression_type(mut self, value: i8) -> Self {
        self.compression_type = value;
        self
    }
    /// Sets `metrics` to the passed value.
    ///
    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    ///
    /// Supported API versions: 0
    pub fn with_metrics(mut self, value: Bytes) -> Self {
        self.metrics = value;
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
impl Encodable for PushTelemetryRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Uuid.encode(buf, &self.client_instance_id)?;
        types::Int32.encode(buf, &self.subscription_id)?;
        types::Boolean.encode(buf, &self.terminating)?;
        types::Int8.encode(buf, &self.compression_type)?;
        types::CompactBytes.encode(buf, &self.metrics)?;
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
        total_size += types::Uuid.compute_size(&self.client_instance_id)?;
        total_size += types::Int32.compute_size(&self.subscription_id)?;
        total_size += types::Boolean.compute_size(&self.terminating)?;
        total_size += types::Int8.compute_size(&self.compression_type)?;
        total_size += types::CompactBytes.compute_size(&self.metrics)?;
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
impl Decodable for PushTelemetryRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let client_instance_id = types::Uuid.decode(buf)?;
        let subscription_id = types::Int32.decode(buf)?;
        let terminating = types::Boolean.decode(buf)?;
        let compression_type = types::Int8.decode(buf)?;
        let metrics = types::CompactBytes.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
            unknown_tagged_fields,
        })
    }
}

impl Default for PushTelemetryRequest {
    fn default() -> Self {
        Self {
            client_instance_id: Uuid::nil(),
            subscription_id: 0,
            terminating: false,
            compression_type: 0,
            metrics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PushTelemetryRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for PushTelemetryRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
