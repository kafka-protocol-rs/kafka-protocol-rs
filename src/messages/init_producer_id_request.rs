//! InitProducerIdRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/InitProducerIdRequest.json).
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
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    ///
    /// Supported API versions: 0-5
    pub transactional_id: Option<super::TransactionalId>,

    /// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    ///
    /// Supported API versions: 0-5
    pub transaction_timeout_ms: i32,

    /// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    ///
    /// Supported API versions: 3-5
    pub producer_id: super::ProducerId,

    /// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    ///
    /// Supported API versions: 3-5
    pub producer_epoch: i16,

    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    ///
    /// Supported API versions: none
    pub enable_2_pc: bool,

    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    ///
    /// Supported API versions: none
    pub keep_prepared_txn: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl InitProducerIdRequest {
    /// Sets `transactional_id` to the passed value.
    ///
    /// The transactional id, or null if the producer is not transactional.
    ///
    /// Supported API versions: 0-5
    pub fn with_transactional_id(mut self, value: Option<super::TransactionalId>) -> Self {
        self.transactional_id = value;
        self
    }
    /// Sets `transaction_timeout_ms` to the passed value.
    ///
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    ///
    /// Supported API versions: 0-5
    pub fn with_transaction_timeout_ms(mut self, value: i32) -> Self {
        self.transaction_timeout_ms = value;
        self
    }
    /// Sets `producer_id` to the passed value.
    ///
    /// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    ///
    /// Supported API versions: 3-5
    pub fn with_producer_id(mut self, value: super::ProducerId) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `producer_epoch` to the passed value.
    ///
    /// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    ///
    /// Supported API versions: 3-5
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    /// Sets `enable_2_pc` to the passed value.
    ///
    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    ///
    /// Supported API versions: none
    pub fn with_enable_2_pc(mut self, value: bool) -> Self {
        self.enable_2_pc = value;
        self
    }
    /// Sets `keep_prepared_txn` to the passed value.
    ///
    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    ///
    /// Supported API versions: none
    pub fn with_keep_prepared_txn(mut self, value: bool) -> Self {
        self.keep_prepared_txn = value;
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
impl Encodable for InitProducerIdRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 2 {
            types::CompactString.encode(buf, &self.transactional_id)?;
        } else {
            types::String.encode(buf, &self.transactional_id)?;
        }
        types::Int32.encode(buf, &self.transaction_timeout_ms)?;
        if version >= 3 {
            types::Int64.encode(buf, &self.producer_id)?;
        } else {
            if self.producer_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int16.encode(buf, &self.producer_epoch)?;
        } else {
            if self.producer_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
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
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.transactional_id)?;
        } else {
            total_size += types::String.compute_size(&self.transactional_id)?;
        }
        total_size += types::Int32.compute_size(&self.transaction_timeout_ms)?;
        if version >= 3 {
            total_size += types::Int64.compute_size(&self.producer_id)?;
        } else {
            if self.producer_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int16.compute_size(&self.producer_epoch)?;
        } else {
            if self.producer_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
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
impl Decodable for InitProducerIdRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let transactional_id = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let transaction_timeout_ms = types::Int32.decode(buf)?;
        let producer_id = if version >= 3 {
            types::Int64.decode(buf)?
        } else {
            (-1).into()
        };
        let producer_epoch = if version >= 3 {
            types::Int16.decode(buf)?
        } else {
            -1
        };
        let enable_2_pc = false;
        let keep_prepared_txn = false;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 2 {
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
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable_2_pc,
            keep_prepared_txn,
            unknown_tagged_fields,
        })
    }
}

impl Default for InitProducerIdRequest {
    fn default() -> Self {
        Self {
            transactional_id: Some(Default::default()),
            transaction_timeout_ms: 0,
            producer_id: (-1).into(),
            producer_epoch: -1,
            enable_2_pc: false,
            keep_prepared_txn: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for InitProducerIdRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for InitProducerIdRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            2
        } else {
            1
        }
    }
}
