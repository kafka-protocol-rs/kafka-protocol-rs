//! SnapshotHeaderRecord
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/SnapshotHeaderRecord.json).
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
pub struct SnapshotHeaderRecord {
    /// The version of the snapshot header record.
    ///
    /// Supported API versions: 0
    pub version: i16,

    /// The append time of the last record from the log contained in this snapshot.
    ///
    /// Supported API versions: 0
    pub last_contained_log_timestamp: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SnapshotHeaderRecord {
    /// Sets `version` to the passed value.
    ///
    /// The version of the snapshot header record.
    ///
    /// Supported API versions: 0
    pub fn with_version(mut self, value: i16) -> Self {
        self.version = value;
        self
    }
    /// Sets `last_contained_log_timestamp` to the passed value.
    ///
    /// The append time of the last record from the log contained in this snapshot.
    ///
    /// Supported API versions: 0
    pub fn with_last_contained_log_timestamp(mut self, value: i64) -> Self {
        self.last_contained_log_timestamp = value;
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

impl Encodable for SnapshotHeaderRecord {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.version)?;
        types::Int64.encode(buf, &self.last_contained_log_timestamp)?;
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
        total_size += types::Int16.compute_size(&self.version)?;
        total_size += types::Int64.compute_size(&self.last_contained_log_timestamp)?;
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

impl Decodable for SnapshotHeaderRecord {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let version = types::Int16.decode(buf)?;
        let last_contained_log_timestamp = types::Int64.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            version,
            last_contained_log_timestamp,
            unknown_tagged_fields,
        })
    }
}

impl Default for SnapshotHeaderRecord {
    fn default() -> Self {
        Self {
            version: 0,
            last_contained_log_timestamp: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SnapshotHeaderRecord {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
