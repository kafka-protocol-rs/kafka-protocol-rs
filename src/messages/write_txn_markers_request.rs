//! WriteTxnMarkersRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/WriteTxnMarkersRequest.json).
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

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct WritableTxnMarker {
    /// The current producer ID.
    ///
    /// Supported API versions: 1
    pub producer_id: super::ProducerId,

    /// The current epoch associated with the producer ID.
    ///
    /// Supported API versions: 1
    pub producer_epoch: i16,

    /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
    ///
    /// Supported API versions: 1
    pub transaction_result: bool,

    /// Each topic that we want to write transaction marker(s) for.
    ///
    /// Supported API versions: 1
    pub topics: Vec<WritableTxnMarkerTopic>,

    /// Epoch associated with the transaction state partition hosted by this transaction coordinator.
    ///
    /// Supported API versions: 1
    pub coordinator_epoch: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl WritableTxnMarker {
    /// Sets `producer_id` to the passed value.
    ///
    /// The current producer ID.
    ///
    /// Supported API versions: 1
    pub fn with_producer_id(mut self, value: super::ProducerId) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `producer_epoch` to the passed value.
    ///
    /// The current epoch associated with the producer ID.
    ///
    /// Supported API versions: 1
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    /// Sets `transaction_result` to the passed value.
    ///
    /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
    ///
    /// Supported API versions: 1
    pub fn with_transaction_result(mut self, value: bool) -> Self {
        self.transaction_result = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Each topic that we want to write transaction marker(s) for.
    ///
    /// Supported API versions: 1
    pub fn with_topics(mut self, value: Vec<WritableTxnMarkerTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `coordinator_epoch` to the passed value.
    ///
    /// Epoch associated with the transaction state partition hosted by this transaction coordinator.
    ///
    /// Supported API versions: 1
    pub fn with_coordinator_epoch(mut self, value: i32) -> Self {
        self.coordinator_epoch = value;
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
impl Encodable for WritableTxnMarker {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.producer_id)?;
        types::Int16.encode(buf, &self.producer_epoch)?;
        types::Boolean.encode(buf, &self.transaction_result)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        types::Int32.encode(buf, &self.coordinator_epoch)?;
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
        total_size += types::Int64.compute_size(&self.producer_id)?;
        total_size += types::Int16.compute_size(&self.producer_epoch)?;
        total_size += types::Boolean.compute_size(&self.transaction_result)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        total_size += types::Int32.compute_size(&self.coordinator_epoch)?;
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
impl Decodable for WritableTxnMarker {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let producer_id = types::Int64.decode(buf)?;
        let producer_epoch = types::Int16.decode(buf)?;
        let transaction_result = types::Boolean.decode(buf)?;
        let topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let coordinator_epoch = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for WritableTxnMarker {
    fn default() -> Self {
        Self {
            producer_id: (0).into(),
            producer_epoch: 0,
            transaction_result: false,
            topics: Default::default(),
            coordinator_epoch: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for WritableTxnMarker {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct WritableTxnMarkerTopic {
    /// The topic name.
    ///
    /// Supported API versions: 1
    pub name: super::TopicName,

    /// The indexes of the partitions to write transaction markers for.
    ///
    /// Supported API versions: 1
    pub partition_indexes: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl WritableTxnMarkerTopic {
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 1
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partition_indexes` to the passed value.
    ///
    /// The indexes of the partitions to write transaction markers for.
    ///
    /// Supported API versions: 1
    pub fn with_partition_indexes(mut self, value: Vec<i32>) -> Self {
        self.partition_indexes = value;
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
impl Encodable for WritableTxnMarkerTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::CompactArray(types::Int32).encode(buf, &self.partition_indexes)?;
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
        total_size += types::CompactArray(types::Int32).compute_size(&self.partition_indexes)?;
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
impl Decodable for WritableTxnMarkerTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let partition_indexes = types::CompactArray(types::Int32).decode(buf)?;
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
            partition_indexes,
            unknown_tagged_fields,
        })
    }
}

impl Default for WritableTxnMarkerTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partition_indexes: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for WritableTxnMarkerTopic {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct WriteTxnMarkersRequest {
    /// The transaction markers to be written.
    ///
    /// Supported API versions: 1
    pub markers: Vec<WritableTxnMarker>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl WriteTxnMarkersRequest {
    /// Sets `markers` to the passed value.
    ///
    /// The transaction markers to be written.
    ///
    /// Supported API versions: 1
    pub fn with_markers(mut self, value: Vec<WritableTxnMarker>) -> Self {
        self.markers = value;
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
impl Encodable for WriteTxnMarkersRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.markers)?;
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
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.markers)?;
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
impl Decodable for WriteTxnMarkersRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 1 {
            bail!("specified version not supported by this message type");
        }
        let markers = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            markers,
            unknown_tagged_fields,
        })
    }
}

impl Default for WriteTxnMarkersRequest {
    fn default() -> Self {
        Self {
            markers: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for WriteTxnMarkersRequest {
    const VERSIONS: VersionRange = VersionRange { min: 1, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for WriteTxnMarkersRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
