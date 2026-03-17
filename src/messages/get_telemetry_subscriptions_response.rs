//! GetTelemetrySubscriptionsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/GetTelemetrySubscriptionsResponse.json).
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
pub struct GetTelemetrySubscriptionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
    ///
    /// Supported API versions: 0
    pub client_instance_id: Uuid,

    /// Unique identifier for the current subscription set for this client instance.
    ///
    /// Supported API versions: 0
    pub subscription_id: i32,

    /// Compression types that broker accepts for the PushTelemetryRequest.
    ///
    /// Supported API versions: 0
    pub accepted_compression_types: Vec<i8>,

    /// Configured push interval, which is the lowest configured interval in the current subscription set.
    ///
    /// Supported API versions: 0
    pub push_interval_ms: i32,

    /// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
    ///
    /// Supported API versions: 0
    pub telemetry_max_bytes: i32,

    /// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
    ///
    /// Supported API versions: 0
    pub delta_temporality: bool,

    /// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
    ///
    /// Supported API versions: 0
    pub requested_metrics: Vec<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl GetTelemetrySubscriptionsResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `client_instance_id` to the passed value.
    ///
    /// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
    ///
    /// Supported API versions: 0
    pub fn with_client_instance_id(mut self, value: Uuid) -> Self {
        self.client_instance_id = value;
        self
    }
    /// Sets `subscription_id` to the passed value.
    ///
    /// Unique identifier for the current subscription set for this client instance.
    ///
    /// Supported API versions: 0
    pub fn with_subscription_id(mut self, value: i32) -> Self {
        self.subscription_id = value;
        self
    }
    /// Sets `accepted_compression_types` to the passed value.
    ///
    /// Compression types that broker accepts for the PushTelemetryRequest.
    ///
    /// Supported API versions: 0
    pub fn with_accepted_compression_types(mut self, value: Vec<i8>) -> Self {
        self.accepted_compression_types = value;
        self
    }
    /// Sets `push_interval_ms` to the passed value.
    ///
    /// Configured push interval, which is the lowest configured interval in the current subscription set.
    ///
    /// Supported API versions: 0
    pub fn with_push_interval_ms(mut self, value: i32) -> Self {
        self.push_interval_ms = value;
        self
    }
    /// Sets `telemetry_max_bytes` to the passed value.
    ///
    /// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
    ///
    /// Supported API versions: 0
    pub fn with_telemetry_max_bytes(mut self, value: i32) -> Self {
        self.telemetry_max_bytes = value;
        self
    }
    /// Sets `delta_temporality` to the passed value.
    ///
    /// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
    ///
    /// Supported API versions: 0
    pub fn with_delta_temporality(mut self, value: bool) -> Self {
        self.delta_temporality = value;
        self
    }
    /// Sets `requested_metrics` to the passed value.
    ///
    /// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
    ///
    /// Supported API versions: 0
    pub fn with_requested_metrics(mut self, value: Vec<StrBytes>) -> Self {
        self.requested_metrics = value;
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
impl Encodable for GetTelemetrySubscriptionsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Uuid.encode(buf, &self.client_instance_id)?;
        types::Int32.encode(buf, &self.subscription_id)?;
        types::CompactArray(types::Int8).encode(buf, &self.accepted_compression_types)?;
        types::Int32.encode(buf, &self.push_interval_ms)?;
        types::Int32.encode(buf, &self.telemetry_max_bytes)?;
        types::Boolean.encode(buf, &self.delta_temporality)?;
        types::CompactArray(types::CompactString).encode(buf, &self.requested_metrics)?;
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
        total_size += types::Uuid.compute_size(&self.client_instance_id)?;
        total_size += types::Int32.compute_size(&self.subscription_id)?;
        total_size +=
            types::CompactArray(types::Int8).compute_size(&self.accepted_compression_types)?;
        total_size += types::Int32.compute_size(&self.push_interval_ms)?;
        total_size += types::Int32.compute_size(&self.telemetry_max_bytes)?;
        total_size += types::Boolean.compute_size(&self.delta_temporality)?;
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.requested_metrics)?;
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
impl Decodable for GetTelemetrySubscriptionsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let client_instance_id = types::Uuid.decode(buf)?;
        let subscription_id = types::Int32.decode(buf)?;
        let accepted_compression_types = types::CompactArray(types::Int8).decode(buf)?;
        let push_interval_ms = types::Int32.decode(buf)?;
        let telemetry_max_bytes = types::Int32.decode(buf)?;
        let delta_temporality = types::Boolean.decode(buf)?;
        let requested_metrics = types::CompactArray(types::CompactString).decode(buf)?;
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
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
            unknown_tagged_fields,
        })
    }
}

impl Default for GetTelemetrySubscriptionsResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            client_instance_id: Uuid::nil(),
            subscription_id: 0,
            accepted_compression_types: Default::default(),
            push_interval_ms: 0,
            telemetry_max_bytes: 0,
            delta_temporality: false,
            requested_metrics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for GetTelemetrySubscriptionsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for GetTelemetrySubscriptionsResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
