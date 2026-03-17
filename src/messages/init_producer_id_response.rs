//! InitProducerIdResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/InitProducerIdResponse.json).
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

/// Valid versions: 0-6
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct InitProducerIdResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-6
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-6
    pub error_code: i16,

    /// The current producer id.
    ///
    /// Supported API versions: 0-6
    pub producer_id: super::ProducerId,

    /// The current epoch associated with the producer id.
    ///
    /// Supported API versions: 0-6
    pub producer_epoch: i16,

    /// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    ///
    /// Supported API versions: 6
    pub ongoing_txn_producer_id: super::ProducerId,

    /// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    ///
    /// Supported API versions: 6
    pub ongoing_txn_producer_epoch: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl InitProducerIdResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0-6
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-6
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `producer_id` to the passed value.
    ///
    /// The current producer id.
    ///
    /// Supported API versions: 0-6
    pub fn with_producer_id(mut self, value: super::ProducerId) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `producer_epoch` to the passed value.
    ///
    /// The current epoch associated with the producer id.
    ///
    /// Supported API versions: 0-6
    pub fn with_producer_epoch(mut self, value: i16) -> Self {
        self.producer_epoch = value;
        self
    }
    /// Sets `ongoing_txn_producer_id` to the passed value.
    ///
    /// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    ///
    /// Supported API versions: 6
    pub fn with_ongoing_txn_producer_id(mut self, value: super::ProducerId) -> Self {
        self.ongoing_txn_producer_id = value;
        self
    }
    /// Sets `ongoing_txn_producer_epoch` to the passed value.
    ///
    /// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    ///
    /// Supported API versions: 6
    pub fn with_ongoing_txn_producer_epoch(mut self, value: i16) -> Self {
        self.ongoing_txn_producer_epoch = value;
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
impl Encodable for InitProducerIdResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::Int64.encode(buf, &self.producer_id)?;
        types::Int16.encode(buf, &self.producer_epoch)?;
        if version >= 6 {
            types::Int64.encode(buf, &self.ongoing_txn_producer_id)?;
        } else {
            if self.ongoing_txn_producer_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            types::Int16.encode(buf, &self.ongoing_txn_producer_epoch)?;
        } else {
            if self.ongoing_txn_producer_epoch != -1 {
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
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int64.compute_size(&self.producer_id)?;
        total_size += types::Int16.compute_size(&self.producer_epoch)?;
        if version >= 6 {
            total_size += types::Int64.compute_size(&self.ongoing_txn_producer_id)?;
        } else {
            if self.ongoing_txn_producer_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            total_size += types::Int16.compute_size(&self.ongoing_txn_producer_epoch)?;
        } else {
            if self.ongoing_txn_producer_epoch != -1 {
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

#[cfg(feature = "client")]
impl Decodable for InitProducerIdResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let producer_id = types::Int64.decode(buf)?;
        let producer_epoch = types::Int16.decode(buf)?;
        let ongoing_txn_producer_id = if version >= 6 {
            types::Int64.decode(buf)?
        } else {
            (-1).into()
        };
        let ongoing_txn_producer_epoch = if version >= 6 {
            types::Int16.decode(buf)?
        } else {
            -1
        };
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
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for InitProducerIdResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            producer_id: (-1).into(),
            producer_epoch: 0,
            ongoing_txn_producer_id: (-1).into(),
            ongoing_txn_producer_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for InitProducerIdResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for InitProducerIdResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 2 {
            1
        } else {
            0
        }
    }
}
