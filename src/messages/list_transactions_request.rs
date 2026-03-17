//! ListTransactionsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ListTransactionsRequest.json).
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
pub struct ListTransactionsRequest {
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
    ///
    /// Supported API versions: 0-2
    pub state_filters: Vec<StrBytes>,

    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
    ///
    /// Supported API versions: 0-2
    pub producer_id_filters: Vec<super::ProducerId>,

    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
    ///
    /// Supported API versions: 1-2
    pub duration_filter: i64,

    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
    ///
    /// Supported API versions: 2
    pub transactional_id_pattern: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ListTransactionsRequest {
    /// Sets `state_filters` to the passed value.
    ///
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
    ///
    /// Supported API versions: 0-2
    pub fn with_state_filters(mut self, value: Vec<StrBytes>) -> Self {
        self.state_filters = value;
        self
    }
    /// Sets `producer_id_filters` to the passed value.
    ///
    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
    ///
    /// Supported API versions: 0-2
    pub fn with_producer_id_filters(mut self, value: Vec<super::ProducerId>) -> Self {
        self.producer_id_filters = value;
        self
    }
    /// Sets `duration_filter` to the passed value.
    ///
    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
    ///
    /// Supported API versions: 1-2
    pub fn with_duration_filter(mut self, value: i64) -> Self {
        self.duration_filter = value;
        self
    }
    /// Sets `transactional_id_pattern` to the passed value.
    ///
    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
    ///
    /// Supported API versions: 2
    pub fn with_transactional_id_pattern(mut self, value: Option<StrBytes>) -> Self {
        self.transactional_id_pattern = value;
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
impl Encodable for ListTransactionsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::CompactString).encode(buf, &self.state_filters)?;
        types::CompactArray(types::Int64).encode(buf, &self.producer_id_filters)?;
        if version >= 1 {
            types::Int64.encode(buf, &self.duration_filter)?;
        } else {
            if self.duration_filter != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            types::CompactString.encode(buf, &self.transactional_id_pattern)?;
        } else {
            if !self.transactional_id_pattern.is_none() {
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
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.state_filters)?;
        total_size += types::CompactArray(types::Int64).compute_size(&self.producer_id_filters)?;
        if version >= 1 {
            total_size += types::Int64.compute_size(&self.duration_filter)?;
        } else {
            if self.duration_filter != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            total_size += types::CompactString.compute_size(&self.transactional_id_pattern)?;
        } else {
            if !self.transactional_id_pattern.is_none() {
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

#[cfg(feature = "broker")]
impl Decodable for ListTransactionsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let state_filters = types::CompactArray(types::CompactString).decode(buf)?;
        let producer_id_filters = types::CompactArray(types::Int64).decode(buf)?;
        let duration_filter = if version >= 1 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let transactional_id_pattern = if version >= 2 {
            types::CompactString.decode(buf)?
        } else {
            None
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
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
            unknown_tagged_fields,
        })
    }
}

impl Default for ListTransactionsRequest {
    fn default() -> Self {
        Self {
            state_filters: Default::default(),
            producer_id_filters: Default::default(),
            duration_filter: -1,
            transactional_id_pattern: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ListTransactionsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ListTransactionsRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
