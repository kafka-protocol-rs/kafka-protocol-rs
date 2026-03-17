//! EndTxnMarker
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/EndTxnMarker.json).
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
pub struct EndTxnMarker {
    /// The coordinator epoch when appending the record
    ///
    /// Supported API versions: 0
    pub coordinator_epoch: i32,
}

impl EndTxnMarker {
    /// Sets `coordinator_epoch` to the passed value.
    ///
    /// The coordinator epoch when appending the record
    ///
    /// Supported API versions: 0
    pub fn with_coordinator_epoch(mut self, value: i32) -> Self {
        self.coordinator_epoch = value;
        self
    }
}

impl Encodable for EndTxnMarker {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.coordinator_epoch)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.coordinator_epoch)?;

        Ok(total_size)
    }
}

impl Decodable for EndTxnMarker {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let coordinator_epoch = types::Int32.decode(buf)?;
        Ok(Self { coordinator_epoch })
    }
}

impl Default for EndTxnMarker {
    fn default() -> Self {
        Self {
            coordinator_epoch: 0,
        }
    }
}

impl Message for EndTxnMarker {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
