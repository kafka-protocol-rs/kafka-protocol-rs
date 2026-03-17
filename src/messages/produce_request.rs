//! ProduceRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ProduceRequest.json).
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
pub struct PartitionProduceData {
    /// The partition index.
    ///
    /// Supported API versions: 3-13
    pub index: i32,

    /// The record data to be produced.
    ///
    /// Supported API versions: 3-13
    pub records: Option<Bytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl PartitionProduceData {
    /// Sets `index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 3-13
    pub fn with_index(mut self, value: i32) -> Self {
        self.index = value;
        self
    }
    /// Sets `records` to the passed value.
    ///
    /// The record data to be produced.
    ///
    /// Supported API versions: 3-13
    pub fn with_records(mut self, value: Option<Bytes>) -> Self {
        self.records = value;
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
impl Encodable for PartitionProduceData {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.index)?;
        if version >= 9 {
            types::CompactBytes.encode(buf, &self.records)?;
        } else {
            types::Bytes.encode(buf, &self.records)?;
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
        total_size += types::Int32.compute_size(&self.index)?;
        if version >= 9 {
            total_size += types::CompactBytes.compute_size(&self.records)?;
        } else {
            total_size += types::Bytes.compute_size(&self.records)?;
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

#[cfg(feature = "broker")]
impl Decodable for PartitionProduceData {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let index = types::Int32.decode(buf)?;
        let records = if version >= 9 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
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
            index,
            records,
            unknown_tagged_fields,
        })
    }
}

impl Default for PartitionProduceData {
    fn default() -> Self {
        Self {
            index: 0,
            records: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for PartitionProduceData {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ProduceRequest {
    /// The transactional ID, or null if the producer is not transactional.
    ///
    /// Supported API versions: 3-13
    pub transactional_id: Option<super::TransactionalId>,

    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    ///
    /// Supported API versions: 3-13
    pub acks: i16,

    /// The timeout to await a response in milliseconds.
    ///
    /// Supported API versions: 3-13
    pub timeout_ms: i32,

    /// Each topic to produce to.
    ///
    /// Supported API versions: 3-13
    pub topic_data: Vec<TopicProduceData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ProduceRequest {
    /// Sets `transactional_id` to the passed value.
    ///
    /// The transactional ID, or null if the producer is not transactional.
    ///
    /// Supported API versions: 3-13
    pub fn with_transactional_id(mut self, value: Option<super::TransactionalId>) -> Self {
        self.transactional_id = value;
        self
    }
    /// Sets `acks` to the passed value.
    ///
    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    ///
    /// Supported API versions: 3-13
    pub fn with_acks(mut self, value: i16) -> Self {
        self.acks = value;
        self
    }
    /// Sets `timeout_ms` to the passed value.
    ///
    /// The timeout to await a response in milliseconds.
    ///
    /// Supported API versions: 3-13
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    /// Sets `topic_data` to the passed value.
    ///
    /// Each topic to produce to.
    ///
    /// Supported API versions: 3-13
    pub fn with_topic_data(mut self, value: Vec<TopicProduceData>) -> Self {
        self.topic_data = value;
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
impl Encodable for ProduceRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 9 {
            types::CompactString.encode(buf, &self.transactional_id)?;
        } else {
            types::String.encode(buf, &self.transactional_id)?;
        }
        types::Int16.encode(buf, &self.acks)?;
        types::Int32.encode(buf, &self.timeout_ms)?;
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topic_data)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topic_data)?;
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
        if version >= 9 {
            total_size += types::CompactString.compute_size(&self.transactional_id)?;
        } else {
            total_size += types::String.compute_size(&self.transactional_id)?;
        }
        total_size += types::Int16.compute_size(&self.acks)?;
        total_size += types::Int32.compute_size(&self.timeout_ms)?;
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topic_data)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topic_data)?;
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

#[cfg(feature = "broker")]
impl Decodable for ProduceRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 3 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let transactional_id = if version >= 9 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let acks = types::Int16.decode(buf)?;
        let timeout_ms = types::Int32.decode(buf)?;
        let topic_data = if version >= 9 {
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
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
            unknown_tagged_fields,
        })
    }
}

impl Default for ProduceRequest {
    fn default() -> Self {
        Self {
            transactional_id: None,
            acks: 0,
            timeout_ms: 0,
            topic_data: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ProduceRequest {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 3-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicProduceData {
    /// The topic name.
    ///
    /// Supported API versions: 3-12
    pub name: super::TopicName,

    /// The unique topic ID
    ///
    /// Supported API versions: 13
    pub topic_id: Uuid,

    /// Each partition to produce to.
    ///
    /// Supported API versions: 3-13
    pub partition_data: Vec<PartitionProduceData>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicProduceData {
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
    /// Sets `partition_data` to the passed value.
    ///
    /// Each partition to produce to.
    ///
    /// Supported API versions: 3-13
    pub fn with_partition_data(mut self, value: Vec<PartitionProduceData>) -> Self {
        self.partition_data = value;
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
impl Encodable for TopicProduceData {
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
            types::CompactArray(types::Struct { version }).encode(buf, &self.partition_data)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partition_data)?;
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
                .compute_size(&self.partition_data)?;
        } else {
            total_size +=
                types::Array(types::Struct { version }).compute_size(&self.partition_data)?;
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

#[cfg(feature = "broker")]
impl Decodable for TopicProduceData {
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
        let partition_data = if version >= 9 {
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
            partition_data,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicProduceData {
    fn default() -> Self {
        Self {
            name: Default::default(),
            topic_id: Uuid::nil(),
            partition_data: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicProduceData {
    const VERSIONS: VersionRange = VersionRange { min: 3, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ProduceRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 9 {
            2
        } else {
            1
        }
    }
}
