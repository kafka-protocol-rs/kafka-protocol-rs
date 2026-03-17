//! ProduceResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ProduceResponse.json).
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

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub batch_index: i32,

    /// The error message of the record that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub batch_index_error_message: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl BatchIndexAndErrorMessage {
    /// Sets `batch_index` to the passed value.
    ///
    /// The batch index of the record that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub fn with_batch_index(mut self, value: i32) -> Self {
        self.batch_index = value;
        self
    }
    /// Sets `batch_index_error_message` to the passed value.
    ///
    /// The error message of the record that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub fn with_batch_index_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.batch_index_error_message = value;
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
impl Encodable for BatchIndexAndErrorMessage {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 8 {
            types::Int32.encode(buf, &self.batch_index)?;
        } else {
            if self.batch_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            if version >= 9 {
                types::CompactString.encode(buf, &self.batch_index_error_message)?;
            } else {
                types::String.encode(buf, &self.batch_index_error_message)?;
            }
        } else {
            if !self.batch_index_error_message.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
        if version >= 8 {
            total_size += types::Int32.compute_size(&self.batch_index)?;
        } else {
            if self.batch_index != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            if version >= 9 {
                total_size += types::CompactString.compute_size(&self.batch_index_error_message)?;
            } else {
                total_size += types::String.compute_size(&self.batch_index_error_message)?;
            }
        } else {
            if !self.batch_index_error_message.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
impl Decodable for BatchIndexAndErrorMessage {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let batch_index = if version >= 8 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let batch_index_error_message = if version >= 8 {
            if version >= 9 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            None
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            batch_index,
            batch_index_error_message,
            unknown_tagged_fields,
        })
    }
}

impl Default for BatchIndexAndErrorMessage {
    fn default() -> Self {
        Self {
            batch_index: 0,
            batch_index_error_message: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for BatchIndexAndErrorMessage {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 10-13
    pub leader_id: super::BrokerId,

    /// The latest known leader epoch.
    ///
    /// Supported API versions: 10-13
    pub leader_epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl LeaderIdAndEpoch {
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the current leader or -1 if the leader is unknown.
    ///
    /// Supported API versions: 10-13
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The latest known leader epoch.
    ///
    /// Supported API versions: 10-13
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
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
impl Encodable for LeaderIdAndEpoch {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 10 {
            types::Int32.encode(buf, &self.leader_id)?;
        } else {
            if self.leader_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            types::Int32.encode(buf, &self.leader_epoch)?;
        } else {
            if self.leader_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
        if version >= 10 {
            total_size += types::Int32.compute_size(&self.leader_id)?;
        } else {
            if self.leader_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            total_size += types::Int32.compute_size(&self.leader_epoch)?;
        } else {
            if self.leader_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
impl Decodable for LeaderIdAndEpoch {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let leader_id = if version >= 10 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let leader_epoch = if version >= 10 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            leader_id,
            leader_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for LeaderIdAndEpoch {
    fn default() -> Self {
        Self {
            leader_id: (-1).into(),
            leader_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for LeaderIdAndEpoch {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    ///
    /// Supported API versions: 10-13
    pub node_id: super::BrokerId,

    /// The node's hostname.
    ///
    /// Supported API versions: 10-13
    pub host: StrBytes,

    /// The node's port.
    ///
    /// Supported API versions: 10-13
    pub port: i32,

    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 10-13
    pub rack: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl NodeEndpoint {
    /// Sets `node_id` to the passed value.
    ///
    /// The ID of the associated node.
    ///
    /// Supported API versions: 10-13
    pub fn with_node_id(mut self, value: super::BrokerId) -> Self {
        self.node_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The node's hostname.
    ///
    /// Supported API versions: 10-13
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The node's port.
    ///
    /// Supported API versions: 10-13
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack of the node, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 10-13
    pub fn with_rack(mut self, value: Option<StrBytes>) -> Self {
        self.rack = value;
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
impl Encodable for NodeEndpoint {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 10 {
            types::Int32.encode(buf, &self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            types::CompactString.encode(buf, &self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            types::Int32.encode(buf, &self.port)?;
        } else {
            if self.port != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            types::CompactString.encode(buf, &self.rack)?;
        } else {
            if !self.rack.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
        if version >= 10 {
            total_size += types::Int32.compute_size(&self.node_id)?;
        } else {
            if self.node_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            total_size += types::CompactString.compute_size(&self.host)?;
        } else {
            if !self.host.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            total_size += types::Int32.compute_size(&self.port)?;
        } else {
            if self.port != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 10 {
            total_size += types::CompactString.compute_size(&self.rack)?;
        } else {
            if !self.rack.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 9 {
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
impl Decodable for NodeEndpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let node_id = if version >= 10 {
            types::Int32.decode(buf)?
        } else {
            (0).into()
        };
        let host = if version >= 10 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let port = if version >= 10 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let rack = if version >= 10 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            node_id,
            host,
            port,
            rack,
            unknown_tagged_fields,
        })
    }
}

impl Default for NodeEndpoint {
    fn default() -> Self {
        Self {
            node_id: (0).into(),
            host: Default::default(),
            port: 0,
            rack: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for NodeEndpoint {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    ///
    /// Supported API versions: 3-13
    pub index: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 3-13
    pub error_code: i16,

    /// The base offset.
    ///
    /// Supported API versions: 3-13
    pub base_offset: i64,

    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    ///
    /// Supported API versions: 3-13
    pub log_append_time_ms: i64,

    /// The log start offset.
    ///
    /// Supported API versions: 5-13
    pub log_start_offset: i64,

    /// The batch indices of records that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub record_errors: Vec<BatchIndexAndErrorMessage>,

    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub error_message: Option<StrBytes>,

    /// The leader broker that the producer should use for future requests.
    ///
    /// Supported API versions: 10-13
    pub current_leader: LeaderIdAndEpoch,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionProduceResponse {
    /// Sets `index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 3-13
    pub fn with_index(mut self, value: i32) -> Self {
        self.index = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 3-13
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `base_offset` to the passed value.
    ///
    /// The base offset.
    ///
    /// Supported API versions: 3-13
    pub fn with_base_offset(mut self, value: i64) -> Self {
        self.base_offset = value;
        self
    }
    /// Sets `log_append_time_ms` to the passed value.
    ///
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    ///
    /// Supported API versions: 3-13
    pub fn with_log_append_time_ms(mut self, value: i64) -> Self {
        self.log_append_time_ms = value;
        self
    }
    /// Sets `log_start_offset` to the passed value.
    ///
    /// The log start offset.
    ///
    /// Supported API versions: 5-13
    pub fn with_log_start_offset(mut self, value: i64) -> Self {
        self.log_start_offset = value;
        self
    }
    /// Sets `record_errors` to the passed value.
    ///
    /// The batch indices of records that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub fn with_record_errors(mut self, value: Vec<BatchIndexAndErrorMessage>) -> Self {
        self.record_errors = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
    ///
    /// Supported API versions: 8-13
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `current_leader` to the passed value.
    ///
    /// The leader broker that the producer should use for future requests.
    ///
    /// Supported API versions: 10-13
    pub fn with_current_leader(mut self, value: LeaderIdAndEpoch) -> Self {
        self.current_leader = value;
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
impl Encodable for PartitionProduceResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.index)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Int64.encode(buf, &self.base_offset)?;
        types::Int64.encode(buf, &self.log_append_time_ms)?;
        if version >= 5 {
            types::Int64.encode(buf, &self.log_start_offset)?;
        }
        if version >= 8 {
            if version >= 9 {
                types::CompactArray(types::Struct { version }).encode(buf, &self.record_errors)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.record_errors)?;
            }
        }
        if version >= 8 {
            if version >= 9 {
                types::CompactString.encode(buf, &self.error_message)?;
            } else {
                types::String.encode(buf, &self.error_message)?;
            }
        }
        if version >= 9 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 10 {
                if &self.current_leader != &Default::default() {
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
            if version >= 10 {
                if &self.current_leader != &Default::default() {
                    let computed_size =
                        types::Struct { version }.compute_size(&self.current_leader)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    types::UnsignedVarInt.encode(buf, 0)?;
                    types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                    types::Struct { version }.encode(buf, &self.current_leader)?;
                }
            }
            write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.index)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int64.compute_size(&self.base_offset)?;
        total_size += types::Int64.compute_size(&self.log_append_time_ms)?;
        if version >= 5 {
            total_size += types::Int64.compute_size(&self.log_start_offset)?;
        }
        if version >= 8 {
            if version >= 9 {
                total_size += types::CompactArray(types::Struct { version })
                    .compute_size(&self.record_errors)?;
            } else {
                total_size +=
                    types::Array(types::Struct { version }).compute_size(&self.record_errors)?;
            }
        }
        if version >= 8 {
            if version >= 9 {
                total_size += types::CompactString.compute_size(&self.error_message)?;
            } else {
                total_size += types::String.compute_size(&self.error_message)?;
            }
        }
        if version >= 9 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 10 {
                if &self.current_leader != &Default::default() {
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
            if version >= 10 {
                if &self.current_leader != &Default::default() {
                    let computed_size =
                        types::Struct { version }.compute_size(&self.current_leader)?;
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
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for PartitionProduceResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let index = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let base_offset = types::Int64.decode(buf)?;
        let log_append_time_ms = types::Int64.decode(buf)?;
        let log_start_offset = if version >= 5 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let record_errors = if version >= 8 {
            if version >= 9 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
            }
        } else {
            Default::default()
        };
        let error_message = if version >= 8 {
            if version >= 9 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            None
        };
        let mut current_leader = Default::default();
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        if version >= 10 {
                            current_leader = types::Struct { version }.decode(buf)?;
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
        }
        Ok(Self {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionProduceResponse {
    fn default() -> Self {
        Self {
            index: 0,
            error_code: 0,
            base_offset: 0,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: Default::default(),
            error_message: None,
            current_leader: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionProduceResponse {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ProduceResponse {
    /// Each produce response.
    ///
    /// Supported API versions: 3-13
    pub responses: Vec<TopicProduceResponse>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-13
    pub throttle_time_ms: i32,

    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    ///
    /// Supported API versions: 10-13
    pub node_endpoints: Vec<NodeEndpoint>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ProduceResponse {
    /// Sets `responses` to the passed value.
    ///
    /// Each produce response.
    ///
    /// Supported API versions: 3-13
    pub fn with_responses(mut self, value: Vec<TopicProduceResponse>) -> Self {
        self.responses = value;
        self
    }
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-13
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `node_endpoints` to the passed value.
    ///
    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    ///
    /// Supported API versions: 10-13
    pub fn with_node_endpoints(mut self, value: Vec<NodeEndpoint>) -> Self {
        self.node_endpoints = value;
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
impl Encodable for ProduceResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.responses)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.responses)?;
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        if version >= 9 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 10 {
                if !self.node_endpoints.is_empty() {
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
            if version >= 10 {
                if !self.node_endpoints.is_empty() {
                    let computed_size = types::CompactArray(types::Struct { version })
                        .compute_size(&self.node_endpoints)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    types::UnsignedVarInt.encode(buf, 0)?;
                    types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                    types::CompactArray(types::Struct { version })
                        .encode(buf, &self.node_endpoints)?;
                }
            }
            write_unknown_tagged_fields(buf, 1.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.responses)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.responses)?;
        }
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        if version >= 9 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 10 {
                if !self.node_endpoints.is_empty() {
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
            if version >= 10 {
                if !self.node_endpoints.is_empty() {
                    let computed_size = types::CompactArray(types::Struct { version })
                        .compute_size(&self.node_endpoints)?;
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
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for ProduceResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let responses = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let throttle_time_ms = types::Int32.decode(buf)?;
        let mut node_endpoints = Default::default();
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        if version >= 10 {
                            node_endpoints =
                                types::CompactArray(types::Struct { version }).decode(buf)?;
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
        }
        Ok(Self {
            responses,
            throttle_time_ms,
            node_endpoints,
            unknown_tagged_fields,
        })
    }
}

impl Default for ProduceResponse {
    fn default() -> Self {
        Self {
            responses: Default::default(),
            throttle_time_ms: 0,
            node_endpoints: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ProduceResponse {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicProduceResponse {
    /// The topic name.
    ///
    /// Supported API versions: 3-12
    pub name: super::TopicName,

    /// The unique topic ID
    ///
    /// Supported API versions: 13
    pub topic_id: Uuid,

    /// Each partition that we produced to within the topic.
    ///
    /// Supported API versions: 3-13
    pub partition_responses: Vec<PartitionProduceResponse>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicProduceResponse {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 3-12
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID
    ///
    /// Supported API versions: 13
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partition_responses` to the passed value.
    ///
    /// Each partition that we produced to within the topic.
    ///
    /// Supported API versions: 3-13
    pub fn with_partition_responses(mut self, value: Vec<PartitionProduceResponse>) -> Self {
        self.partition_responses = value;
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
impl Encodable for TopicProduceResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version <= 12 {
            if version >= 9 {
                types::CompactString.encode(buf, &self.name)?;
            } else {
                types::String.encode(buf, &self.name)?;
            }
        }
        if version >= 13 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version })
                .encode(buf, &self.partition_responses)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partition_responses)?;
        }
        if version >= 9 {
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
        if version <= 12 {
            if version >= 9 {
                total_size += types::CompactString.compute_size(&self.name)?;
            } else {
                total_size += types::String.compute_size(&self.name)?;
            }
        }
        if version >= 13 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 9 {
            total_size += types::CompactArray(types::Struct { version })
                .compute_size(&self.partition_responses)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.partition_responses)?;
        }
        if version >= 9 {
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
impl Decodable for TopicProduceResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let name = if version <= 12 {
            if version >= 9 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let topic_id = if version >= 13 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let partition_responses = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 9 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            name,
            topic_id,
            partition_responses,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicProduceResponse {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            partition_responses: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicProduceResponse {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ProduceResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 9 {
            1
        } else {
            0
        }
    }
}
