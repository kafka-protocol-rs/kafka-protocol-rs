//! AddPartitionsToTxnRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/AddPartitionsToTxnRequest.json).
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

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnRequest {
    /// List of transactions to add partitions to.
    ///
    /// Supported API versions: 4-5
    pub transactions: Vec<AddPartitionsToTxnTransaction>,

    /// The transactional id corresponding to the transaction.
    ///
    /// Supported API versions: 0-3
    pub v3_and_below_transactional_id: super::TransactionalId,

    /// Current producer id in use by the transactional id.
    ///
    /// Supported API versions: 0-3
    pub v3_and_below_producer_id: super::ProducerId,

    /// Current epoch associated with the producer id.
    ///
    /// Supported API versions: 0-3
    pub v3_and_below_producer_epoch: i16,

    /// The partitions to add to the transaction.
    ///
    /// Supported API versions: 0-3
    pub v3_and_below_topics: Vec<AddPartitionsToTxnTopic>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AddPartitionsToTxnRequest {
    /// Sets `transactions` to the passed value.
    ///
    /// List of transactions to add partitions to.
    ///
    /// Supported API versions: 4-5
    pub fn with_transactions(mut self, value: Vec<AddPartitionsToTxnTransaction>) -> Self {
        self.transactions = value;
        self
    }
    /// Sets `v3_and_below_transactional_id` to the passed value.
    ///
    /// The transactional id corresponding to the transaction.
    ///
    /// Supported API versions: 0-3
    pub fn with_v3_and_below_transactional_id(mut self, value: super::TransactionalId) -> Self {
        self.v3_and_below_transactional_id = value;
        self
    }
    /// Sets `v3_and_below_producer_id` to the passed value.
    ///
    /// Current producer id in use by the transactional id.
    ///
    /// Supported API versions: 0-3
    pub fn with_v3_and_below_producer_id(mut self, value: super::ProducerId) -> Self {
        self.v3_and_below_producer_id = value;
        self
    }
    /// Sets `v3_and_below_producer_epoch` to the passed value.
    ///
    /// Current epoch associated with the producer id.
    ///
    /// Supported API versions: 0-3
    pub fn with_v3_and_below_producer_epoch(mut self, value: i16) -> Self {
        self.v3_and_below_producer_epoch = value;
        self
    }
    /// Sets `v3_and_below_topics` to the passed value.
    ///
    /// The partitions to add to the transaction.
    ///
    /// Supported API versions: 0-3
    pub fn with_v3_and_below_topics(mut self, value: Vec<AddPartitionsToTxnTopic>) -> Self {
        self.v3_and_below_topics = value;
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
impl Encodable for AddPartitionsToTxnRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.transactions)?;
        } else {
            if !self.transactions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            if version >= 3 {
                types::CompactString.encode(buf, &self.v3_and_below_transactional_id)?;
            } else {
                types::String.encode(buf, &self.v3_and_below_transactional_id)?;
            }
        } else {
            if !self.v3_and_below_transactional_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            types::Int64.encode(buf, &self.v3_and_below_producer_id)?;
        } else {
            if self.v3_and_below_producer_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            types::Int16.encode(buf, &self.v3_and_below_producer_epoch)?;
        } else {
            if self.v3_and_below_producer_epoch != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            if version >= 3 {
                types::CompactArray(types::Struct { version })
                    .encode(buf, &self.v3_and_below_topics)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.v3_and_below_topics)?;
            }
        } else {
            if !self.v3_and_below_topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
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
        if version >= 4 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.transactions)?;
        } else {
            if !self.transactions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            if version >= 3 {
                total_size +=
                    types::CompactString.compute_size(&self.v3_and_below_transactional_id)?;
            } else {
                total_size += types::String.compute_size(&self.v3_and_below_transactional_id)?;
            }
        } else {
            if !self.v3_and_below_transactional_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            total_size += types::Int64.compute_size(&self.v3_and_below_producer_id)?;
        } else {
            if self.v3_and_below_producer_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            total_size += types::Int16.compute_size(&self.v3_and_below_producer_epoch)?;
        } else {
            if self.v3_and_below_producer_epoch != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version <= 3 {
            if version >= 3 {
                total_size += types::CompactArray(types::Struct { version })
                    .compute_size(&self.v3_and_below_topics)?;
            } else {
                total_size += types::Array(types::Struct { version })
                    .compute_size(&self.v3_and_below_topics)?;
            }
        } else {
            if !self.v3_and_below_topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
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
impl Decodable for AddPartitionsToTxnRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let transactions = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let v3_and_below_transactional_id = if version <= 3 {
            if version >= 3 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let v3_and_below_producer_id = if version <= 3 {
            types::Int64.decode(buf)?
        } else {
            (0).into()
        };
        let v3_and_below_producer_epoch = if version <= 3 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let v3_and_below_topics = if version <= 3 {
            if version >= 3 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
            }
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            transactions,
            v3_and_below_transactional_id,
            v3_and_below_producer_id,
            v3_and_below_producer_epoch,
            v3_and_below_topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for AddPartitionsToTxnRequest {
    fn default() -> Self {
        Self {
            transactions: Default::default(),
            v3_and_below_transactional_id: Default::default(),
            v3_and_below_producer_id: (0).into(),
            v3_and_below_producer_epoch: 0,
            v3_and_below_topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AddPartitionsToTxnRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnTopic {
    /// The name of the topic.
    ///
    /// Supported API versions: 0-5
    pub name: super::TopicName,

    /// The partition indexes to add to the transaction.
    ///
    /// Supported API versions: 0-5
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AddPartitionsToTxnTopic {
    /// Sets `name` to the passed value.
    ///
    /// The name of the topic.
    ///
    /// Supported API versions: 0-5
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partition indexes to add to the transaction.
    ///
    /// Supported API versions: 0-5
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
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
impl Encodable for AddPartitionsToTxnTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 3 {
            types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Int32).encode(buf, &self.partitions)?;
        }
        if version >= 3 {
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
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 3 {
            total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Int32).compute_size(&self.partitions)?;
        }
        if version >= 3 {
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
impl Decodable for AddPartitionsToTxnTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let partitions = if version >= 3 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            types::Array(types::Int32).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
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
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for AddPartitionsToTxnTopic {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AddPartitionsToTxnTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AddPartitionsToTxnTransaction {
    /// The transactional id corresponding to the transaction.
    ///
    /// Supported API versions: 4-5
    pub transactional_id: super::TransactionalId,

    /// Current producer id in use by the transactional id.
    ///
    /// Supported API versions: 4-5
    pub producer_id: super::ProducerId,

    /// Current epoch associated with the producer id.
    ///
    /// Supported API versions: 4-5
    pub producer_epoch: i16,

    /// Boolean to signify if we want to check if the partition is in the transaction rather than add it.
    ///
    /// Supported API versions: 4-5
    pub verify_only: bool,

    /// The partitions to add to the transaction.
    ///
    /// Supported API versions: 4-5
    pub topics: Vec<AddPartitionsToTxnTopic>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl AddPartitionsToTxnTransaction {
    /// Sets `transactional_id` to the passed value.
    ///
    /// The transactional id corresponding to the transaction.
    ///
    /// Supported API versions: 4-5
    pub fn with_transactional_id(mut self, value: super::TransactionalId) -> Self {
        self.transactional_id = value;
        self
    }
    /// Sets `producer_id` to the passed value.
    ///
    /// Current producer id in use by the transactional id.
    ///
    /// Supported API versions: 4-5
    pub fn with_producer_id(mut self, value: super::ProducerId) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `producer_epoch` to the passed value.
    ///
    /// Current epoch associated with the producer id.
    ///
    /// Supported API versions: 4-5
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    /// Sets `verify_only` to the passed value.
    ///
    /// Boolean to signify if we want to check if the partition is in the transaction rather than add it.
    ///
    /// Supported API versions: 4-5
    pub fn with_verify_only(mut self, value: bool) -> Self {
        self.verify_only = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The partitions to add to the transaction.
    ///
    /// Supported API versions: 4-5
    pub fn with_topics(mut self, value: Vec<AddPartitionsToTxnTopic>) -> Self {
        self.topics = value;
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
impl Encodable for AddPartitionsToTxnTransaction {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 4 {
            types::CompactString.encode(buf, &self.transactional_id)?;
        } else {
            if !self.transactional_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            types::Int64.encode(buf, &self.producer_id)?;
        } else {
            if self.producer_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            types::Int16.encode(buf, &self.producer_epoch)?;
        } else {
            if self.producer_epoch != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            types::Boolean.encode(buf, &self.verify_only)?;
        } else {
            if self.verify_only {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
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
        if version >= 4 {
            total_size += types::CompactString.compute_size(&self.transactional_id)?;
        } else {
            if !self.transactional_id.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            total_size += types::Int64.compute_size(&self.producer_id)?;
        } else {
            if self.producer_id != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            total_size += types::Int16.compute_size(&self.producer_epoch)?;
        } else {
            if self.producer_epoch != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            total_size += types::Boolean.compute_size(&self.verify_only)?;
        } else {
            if self.verify_only {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            if !self.topics.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
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
impl Decodable for AddPartitionsToTxnTransaction {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let transactional_id = if version >= 4 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let producer_id = if version >= 4 {
            types::Int64.decode(buf)?
        } else {
            (0).into()
        };
        let producer_epoch = if version >= 4 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let verify_only = if version >= 4 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let topics = if version >= 4 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
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
            producer_id,
            producer_epoch,
            verify_only,
            topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for AddPartitionsToTxnTransaction {
    fn default() -> Self {
        Self {
            transactional_id: Default::default(),
            producer_id: (0).into(),
            producer_epoch: 0,
            verify_only: false,
            topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for AddPartitionsToTxnTransaction {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for AddPartitionsToTxnRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 3 {
            2
        } else {
            1
        }
    }
}
