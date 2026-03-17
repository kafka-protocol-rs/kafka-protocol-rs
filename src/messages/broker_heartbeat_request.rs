//! BrokerHeartbeatRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/BrokerHeartbeatRequest.json).
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

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerHeartbeatRequest {
    /// The broker ID.
    ///
    /// Supported API versions: 0-1
    pub broker_id: super::BrokerId,

    /// The broker epoch.
    ///
    /// Supported API versions: 0-1
    pub broker_epoch: i64,

    /// The highest metadata offset which the broker has reached.
    ///
    /// Supported API versions: 0-1
    pub current_metadata_offset: i64,

    /// True if the broker wants to be fenced, false otherwise.
    ///
    /// Supported API versions: 0-1
    pub want_fence: bool,

    /// True if the broker wants to be shut down, false otherwise.
    ///
    /// Supported API versions: 0-1
    pub want_shut_down: bool,

    /// Log directories that failed and went offline.
    ///
    /// Supported API versions: 1
    pub offline_log_dirs: Vec<Uuid>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl BrokerHeartbeatRequest {
    /// Sets `broker_id` to the passed value.
    ///
    /// The broker ID.
    ///
    /// Supported API versions: 0-1
    pub fn with_broker_id(mut self, value: super::BrokerId) -> Self {
        self.broker_id = value;
        self
    }
    /// Sets `broker_epoch` to the passed value.
    ///
    /// The broker epoch.
    ///
    /// Supported API versions: 0-1
    pub fn with_broker_epoch(mut self, value: i64) -> Self {
        self.broker_epoch = value;
        self
    }
    /// Sets `current_metadata_offset` to the passed value.
    ///
    /// The highest metadata offset which the broker has reached.
    ///
    /// Supported API versions: 0-1
    pub fn with_current_metadata_offset(mut self, value: i64) -> Self {
        self.current_metadata_offset = value;
        self
    }
    /// Sets `want_fence` to the passed value.
    ///
    /// True if the broker wants to be fenced, false otherwise.
    ///
    /// Supported API versions: 0-1
    pub fn with_want_fence(mut self, value: bool) -> Self {
        self.want_fence = value;
        self
    }
    /// Sets `want_shut_down` to the passed value.
    ///
    /// True if the broker wants to be shut down, false otherwise.
    ///
    /// Supported API versions: 0-1
    pub fn with_want_shut_down(mut self, value: bool) -> Self {
        self.want_shut_down = value;
        self
    }
    /// Sets `offline_log_dirs` to the passed value.
    ///
    /// Log directories that failed and went offline.
    ///
    /// Supported API versions: 1
    pub fn with_offline_log_dirs(mut self, value: Vec<Uuid>) -> Self {
        self.offline_log_dirs = value;
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
impl Encodable for BrokerHeartbeatRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.broker_id)?;
        types::Int64.encode(buf, &self.broker_epoch)?;
        types::Int64.encode(buf, &self.current_metadata_offset)?;
        types::Boolean.encode(buf, &self.want_fence)?;
        types::Boolean.encode(buf, &self.want_shut_down)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if !self.offline_log_dirs.is_empty() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
        if version >= 1 {
            if !self.offline_log_dirs.is_empty() {
                let computed_size =
                    types::CompactArray(types::Uuid).compute_size(&self.offline_log_dirs)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::CompactArray(types::Uuid).encode(buf, &self.offline_log_dirs)?;
            }
        }
        write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.broker_id)?;
        total_size += types::Int64.compute_size(&self.broker_epoch)?;
        total_size += types::Int64.compute_size(&self.current_metadata_offset)?;
        total_size += types::Boolean.compute_size(&self.want_fence)?;
        total_size += types::Boolean.compute_size(&self.want_shut_down)?;
        let mut num_tagged_fields = self.unknown_tagged_fields.len();
        if version >= 1 {
            if !self.offline_log_dirs.is_empty() {
                num_tagged_fields += 1;
            }
        }
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
        if version >= 1 {
            if !self.offline_log_dirs.is_empty() {
                let computed_size =
                    types::CompactArray(types::Uuid).compute_size(&self.offline_log_dirs)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(0)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }
        }
        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for BrokerHeartbeatRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let broker_id = types::Int32.decode(buf)?;
        let broker_epoch = types::Int64.decode(buf)?;
        let current_metadata_offset = types::Int64.decode(buf)?;
        let want_fence = types::Boolean.decode(buf)?;
        let want_shut_down = types::Boolean.decode(buf)?;
        let mut offline_log_dirs = Default::default();
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            match tag {
                0 => {
                    if version >= 1 {
                        offline_log_dirs = types::CompactArray(types::Uuid).decode(buf)?;
                    } else {
                        bail!("Tag {} is not valid for version {}", tag, version);
                    }
                }
                _ => {
                    let unknown_value = buf.try_get_bytes(size as usize)?;
                    unknown_tagged_fields.insert(tag as i32, unknown_value);
                }
            }
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            current_metadata_offset,
            want_fence,
            want_shut_down,
            offline_log_dirs,
            unknown_tagged_fields,
        })
    }
}

impl Default for BrokerHeartbeatRequest {
    fn default() -> Self {
        Self {
            broker_id: (0).into(),
            broker_epoch: -1,
            current_metadata_offset: 0,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for BrokerHeartbeatRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for BrokerHeartbeatRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
