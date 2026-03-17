//! FetchRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchRequest.json).
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

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    ///
    /// Supported API versions: 4-18
    pub partition: i32,

    /// The current leader epoch of the partition.
    ///
    /// Supported API versions: 9-18
    pub current_leader_epoch: i32,

    /// The message offset.
    ///
    /// Supported API versions: 4-18
    pub fetch_offset: i64,

    /// The epoch of the last fetched record or -1 if there is none.
    ///
    /// Supported API versions: 12-18
    pub last_fetched_epoch: i32,

    /// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    ///
    /// Supported API versions: 5-18
    pub log_start_offset: i64,

    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 4-18
    pub partition_max_bytes: i32,

    /// The directory id of the follower fetching.
    ///
    /// Supported API versions: 17-18
    pub replica_directory_id: Uuid,

    /// The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
    ///
    /// Supported API versions: 18
    pub high_watermark: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchPartition {
    /// Sets `partition` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 4-18
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    /// Sets `current_leader_epoch` to the passed value.
    ///
    /// The current leader epoch of the partition.
    ///
    /// Supported API versions: 9-18
    pub fn with_current_leader_epoch(mut self, value: i32) -> Self {
        self.current_leader_epoch = value;
        self
    }
    /// Sets `fetch_offset` to the passed value.
    ///
    /// The message offset.
    ///
    /// Supported API versions: 4-18
    pub fn with_fetch_offset(mut self, value: i64) -> Self {
        self.fetch_offset = value;
        self
    }
    /// Sets `last_fetched_epoch` to the passed value.
    ///
    /// The epoch of the last fetched record or -1 if there is none.
    ///
    /// Supported API versions: 12-18
    pub fn with_last_fetched_epoch(mut self, value: i32) -> Self {
        self.last_fetched_epoch = value;
        self
    }
    /// Sets `log_start_offset` to the passed value.
    ///
    /// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    ///
    /// Supported API versions: 5-18
    pub fn with_log_start_offset(mut self, value: i64) -> Self {
        self.log_start_offset = value;
        self
    }
    /// Sets `partition_max_bytes` to the passed value.
    ///
    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 4-18
    pub fn with_partition_max_bytes(mut self, value: i32) -> Self {
        self.partition_max_bytes = value;
        self
    }
    /// Sets `replica_directory_id` to the passed value.
    ///
    /// The directory id of the follower fetching.
    ///
    /// Supported API versions: 17-18
    pub fn with_replica_directory_id(mut self, value: Uuid) -> Self {
        self.replica_directory_id = value;
        self
    }
    /// Sets `high_watermark` to the passed value.
    ///
    /// The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
    ///
    /// Supported API versions: 18
    pub fn with_high_watermark(mut self, value: i64) -> Self {
        self.high_watermark = value;
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
impl Encodable for FetchPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.partition)?;
        if version >= 9 {
            types::Int32.encode(buf, &self.current_leader_epoch)?;
        }
        types::Int64.encode(buf, &self.fetch_offset)?;
        if version >= 12 {
            types::Int32.encode(buf, &self.last_fetched_epoch)?;
        } else {
            if self.last_fetched_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            types::Int64.encode(buf, &self.log_start_offset)?;
        }
        types::Int32.encode(buf, &self.partition_max_bytes)?;
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 17 {
                if &self.replica_directory_id != &Uuid::nil() {
                    num_tagged_fields += 1;
                }
            }
            if version >= 18 {
                if self.high_watermark != 9223372036854775807 {
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
            if version >= 17 {
                if &self.replica_directory_id != &Uuid::nil() {
                    let computed_size = types::Uuid.compute_size(&self.replica_directory_id)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    types::UnsignedVarInt.encode(buf, 0)?;
                    types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                    types::Uuid.encode(buf, &self.replica_directory_id)?;
                }
            }
            if version >= 18 {
                if self.high_watermark != 9223372036854775807 {
                    let computed_size = types::Int64.compute_size(&self.high_watermark)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    types::UnsignedVarInt.encode(buf, 1)?;
                    types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                    types::Int64.encode(buf, &self.high_watermark)?;
                }
            }
            write_unknown_tagged_fields(buf, 2.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.partition)?;
        if version >= 9 {
            total_size += types::Int32.compute_size(&self.current_leader_epoch)?;
        }
        total_size += types::Int64.compute_size(&self.fetch_offset)?;
        if version >= 12 {
            total_size += types::Int32.compute_size(&self.last_fetched_epoch)?;
        } else {
            if self.last_fetched_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 5 {
            total_size += types::Int64.compute_size(&self.log_start_offset)?;
        }
        total_size += types::Int32.compute_size(&self.partition_max_bytes)?;
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if version >= 17 {
                if &self.replica_directory_id != &Uuid::nil() {
                    num_tagged_fields += 1;
                }
            }
            if version >= 18 {
                if self.high_watermark != 9223372036854775807 {
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
            if version >= 17 {
                if &self.replica_directory_id != &Uuid::nil() {
                    let computed_size = types::Uuid.compute_size(&self.replica_directory_id)?;
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
            if version >= 18 {
                if self.high_watermark != 9223372036854775807 {
                    let computed_size = types::Int64.compute_size(&self.high_watermark)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    total_size += types::UnsignedVarInt.compute_size(1)?;
                    total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                    total_size += computed_size;
                }
            }
            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for FetchPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let partition = types::Int32.decode(buf)?;
        let current_leader_epoch = if version >= 9 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let fetch_offset = types::Int64.decode(buf)?;
        let last_fetched_epoch = if version >= 12 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let log_start_offset = if version >= 5 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let partition_max_bytes = types::Int32.decode(buf)?;
        let mut replica_directory_id = Uuid::nil();
        let mut high_watermark = 9223372036854775807;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        if version >= 17 {
                            replica_directory_id = types::Uuid.decode(buf)?;
                        } else {
                            bail!("Tag {} is not valid for version {}", tag, version);
                        }
                    }
                    1 => {
                        if version >= 18 {
                            high_watermark = types::Int64.decode(buf)?;
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
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            replica_directory_id,
            high_watermark,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchPartition {
    fn default() -> Self {
        Self {
            partition: 0,
            current_leader_epoch: -1,
            fetch_offset: 0,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 0,
            replica_directory_id: Uuid::nil(),
            high_watermark: 9223372036854775807,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchPartition {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchRequest {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker registration.
    ///
    /// Supported API versions: 12-18
    pub cluster_id: Option<StrBytes>,

    /// The broker ID of the follower, of -1 if this request is from a consumer.
    ///
    /// Supported API versions: 4-14
    pub replica_id: super::BrokerId,

    /// The state of the replica in the follower.
    ///
    /// Supported API versions: 15-18
    pub replica_state: ReplicaState,

    /// The maximum time in milliseconds to wait for the response.
    ///
    /// Supported API versions: 4-18
    pub max_wait_ms: i32,

    /// The minimum bytes to accumulate in the response.
    ///
    /// Supported API versions: 4-18
    pub min_bytes: i32,

    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 4-18
    pub max_bytes: i32,

    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
    ///
    /// Supported API versions: 4-18
    pub isolation_level: i8,

    /// The fetch session ID.
    ///
    /// Supported API versions: 7-18
    pub session_id: i32,

    /// The fetch session epoch, which is used for ordering requests in a session.
    ///
    /// Supported API versions: 7-18
    pub session_epoch: i32,

    /// The topics to fetch.
    ///
    /// Supported API versions: 4-18
    pub topics: Vec<FetchTopic>,

    /// In an incremental fetch request, the partitions to remove.
    ///
    /// Supported API versions: 7-18
    pub forgotten_topics_data: Vec<ForgottenTopic>,

    /// Rack ID of the consumer making this request.
    ///
    /// Supported API versions: 11-18
    pub rack_id: StrBytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchRequest {
    /// Sets `cluster_id` to the passed value.
    ///
    /// The clusterId if known. This is used to validate metadata fetches prior to broker registration.
    ///
    /// Supported API versions: 12-18
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `replica_id` to the passed value.
    ///
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    ///
    /// Supported API versions: 4-14
    pub fn with_replica_id(mut self, value: super::BrokerId) -> Self {
        self.replica_id = value;
        self
    }
    /// Sets `replica_state` to the passed value.
    ///
    /// The state of the replica in the follower.
    ///
    /// Supported API versions: 15-18
    pub fn with_replica_state(mut self, value: ReplicaState) -> Self {
        self.replica_state = value;
        self
    }
    /// Sets `max_wait_ms` to the passed value.
    ///
    /// The maximum time in milliseconds to wait for the response.
    ///
    /// Supported API versions: 4-18
    pub fn with_max_wait_ms(mut self, value: i32) -> Self {
        self.max_wait_ms = value;
        self
    }
    /// Sets `min_bytes` to the passed value.
    ///
    /// The minimum bytes to accumulate in the response.
    ///
    /// Supported API versions: 4-18
    pub fn with_min_bytes(mut self, value: i32) -> Self {
        self.min_bytes = value;
        self
    }
    /// Sets `max_bytes` to the passed value.
    ///
    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    ///
    /// Supported API versions: 4-18
    pub fn with_max_bytes(mut self, value: i32) -> Self {
        self.max_bytes = value;
        self
    }
    /// Sets `isolation_level` to the passed value.
    ///
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
    ///
    /// Supported API versions: 4-18
    pub fn with_isolation_level(mut self, value: i8) -> Self {
        self.isolation_level = value;
        self
    }
    /// Sets `session_id` to the passed value.
    ///
    /// The fetch session ID.
    ///
    /// Supported API versions: 7-18
    pub fn with_session_id(mut self, value: i32) -> Self {
        self.session_id = value;
        self
    }
    /// Sets `session_epoch` to the passed value.
    ///
    /// The fetch session epoch, which is used for ordering requests in a session.
    ///
    /// Supported API versions: 7-18
    pub fn with_session_epoch(mut self, value: i32) -> Self {
        self.session_epoch = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// The topics to fetch.
    ///
    /// Supported API versions: 4-18
    pub fn with_topics(mut self, value: Vec<FetchTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `forgotten_topics_data` to the passed value.
    ///
    /// In an incremental fetch request, the partitions to remove.
    ///
    /// Supported API versions: 7-18
    pub fn with_forgotten_topics_data(mut self, value: Vec<ForgottenTopic>) -> Self {
        self.forgotten_topics_data = value;
        self
    }
    /// Sets `rack_id` to the passed value.
    ///
    /// Rack ID of the consumer making this request.
    ///
    /// Supported API versions: 11-18
    pub fn with_rack_id(mut self, value: StrBytes) -> Self {
        self.rack_id = value;
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
impl Encodable for FetchRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version <= 14 {
            types::Int32.encode(buf, &self.replica_id)?;
        } else {
            if self.replica_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        types::Int32.encode(buf, &self.max_wait_ms)?;
        types::Int32.encode(buf, &self.min_bytes)?;
        types::Int32.encode(buf, &self.max_bytes)?;
        types::Int8.encode(buf, &self.isolation_level)?;
        if version >= 7 {
            types::Int32.encode(buf, &self.session_id)?;
        }
        if version >= 7 {
            types::Int32.encode(buf, &self.session_epoch)?;
        }
        if version >= 12 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 7 {
            if version >= 12 {
                types::CompactArray(types::Struct { version })
                    .encode(buf, &self.forgotten_topics_data)?;
            } else {
                types::Array(types::Struct { version }).encode(buf, &self.forgotten_topics_data)?;
            }
        } else {
            if !self.forgotten_topics_data.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 11 {
            if version >= 12 {
                types::CompactString.encode(buf, &self.rack_id)?;
            } else {
                types::String.encode(buf, &self.rack_id)?;
            }
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if !self.cluster_id.is_none() {
                num_tagged_fields += 1;
            }
            if version >= 15 {
                if &self.replica_state != &Default::default() {
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
            if !self.cluster_id.is_none() {
                let computed_size = types::CompactString.compute_size(&self.cluster_id)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::CompactString.encode(buf, &self.cluster_id)?;
            }
            if version >= 15 {
                if &self.replica_state != &Default::default() {
                    let computed_size =
                        types::Struct { version }.compute_size(&self.replica_state)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    types::UnsignedVarInt.encode(buf, 1)?;
                    types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                    types::Struct { version }.encode(buf, &self.replica_state)?;
                }
            }
            write_unknown_tagged_fields(buf, 2.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version <= 14 {
            total_size += types::Int32.compute_size(&self.replica_id)?;
        } else {
            if self.replica_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        total_size += types::Int32.compute_size(&self.max_wait_ms)?;
        total_size += types::Int32.compute_size(&self.min_bytes)?;
        total_size += types::Int32.compute_size(&self.max_bytes)?;
        total_size += types::Int8.compute_size(&self.isolation_level)?;
        if version >= 7 {
            total_size += types::Int32.compute_size(&self.session_id)?;
        }
        if version >= 7 {
            total_size += types::Int32.compute_size(&self.session_epoch)?;
        }
        if version >= 12 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 7 {
            if version >= 12 {
                total_size += types::CompactArray(types::Struct { version })
                    .compute_size(&self.forgotten_topics_data)?;
            } else {
                total_size += types::Array(types::Struct { version })
                    .compute_size(&self.forgotten_topics_data)?;
            }
        } else {
            if !self.forgotten_topics_data.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 11 {
            if version >= 12 {
                total_size += types::CompactString.compute_size(&self.rack_id)?;
            } else {
                total_size += types::String.compute_size(&self.rack_id)?;
            }
        }
        if version >= 12 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if !self.cluster_id.is_none() {
                num_tagged_fields += 1;
            }
            if version >= 15 {
                if &self.replica_state != &Default::default() {
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
            if !self.cluster_id.is_none() {
                let computed_size = types::CompactString.compute_size(&self.cluster_id)?;
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
            if version >= 15 {
                if &self.replica_state != &Default::default() {
                    let computed_size =
                        types::Struct { version }.compute_size(&self.replica_state)?;
                    if computed_size > std::u32::MAX as usize {
                        bail!(
                            "Tagged field is too large to encode ({} bytes)",
                            computed_size
                        );
                    }
                    total_size += types::UnsignedVarInt.compute_size(1)?;
                    total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                    total_size += computed_size;
                }
            }
            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for FetchRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let mut cluster_id = None;
        let replica_id = if version <= 14 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let mut replica_state = Default::default();
        let max_wait_ms = types::Int32.decode(buf)?;
        let min_bytes = types::Int32.decode(buf)?;
        let max_bytes = types::Int32.decode(buf)?;
        let isolation_level = types::Int8.decode(buf)?;
        let session_id = if version >= 7 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let session_epoch = if version >= 7 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let topics = if version >= 12 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let forgotten_topics_data = if version >= 7 {
            if version >= 12 {
                types::CompactArray(types::Struct { version }).decode(buf)?
            } else {
                types::Array(types::Struct { version }).decode(buf)?
            }
        } else {
            Default::default()
        };
        let rack_id = if version >= 11 {
            if version >= 12 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            StrBytes::from_static_str("")
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        cluster_id = types::CompactString.decode(buf)?;
                    }
                    1 => {
                        if version >= 15 {
                            replica_state = types::Struct { version }.decode(buf)?;
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
            cluster_id,
            replica_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            replica_id: (-1).into(),
            replica_state: Default::default(),
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0x7fffffff,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: Default::default(),
            forgotten_topics_data: Default::default(),
            rack_id: StrBytes::from_static_str(""),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchRequest {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    /// The name of the topic to fetch.
    ///
    /// Supported API versions: 4-12
    pub topic: super::TopicName,

    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub topic_id: Uuid,

    /// The partitions to fetch.
    ///
    /// Supported API versions: 4-18
    pub partitions: Vec<FetchPartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FetchTopic {
    /// Sets `topic` to the passed value.
    ///
    /// The name of the topic to fetch.
    ///
    /// Supported API versions: 4-12
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions to fetch.
    ///
    /// Supported API versions: 4-18
    pub fn with_partitions(mut self, value: Vec<FetchPartition>) -> Self {
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
impl Encodable for FetchTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version <= 12 {
            if version >= 12 {
                types::CompactString.encode(buf, &self.topic)?;
            } else {
                types::String.encode(buf, &self.topic)?;
            }
        }
        if version >= 13 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 12 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
        }
        if version >= 12 {
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
            if version >= 12 {
                total_size += types::CompactString.compute_size(&self.topic)?;
            } else {
                total_size += types::String.compute_size(&self.topic)?;
            }
        }
        if version >= 13 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 12 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;
        }
        if version >= 12 {
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
impl Decodable for FetchTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let topic = if version <= 12 {
            if version >= 12 {
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
        let partitions = if version >= 12 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for FetchTopic {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FetchTopic {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ForgottenTopic {
    /// The topic name.
    ///
    /// Supported API versions: 7-12
    pub topic: super::TopicName,

    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub topic_id: Uuid,

    /// The partitions indexes to forget.
    ///
    /// Supported API versions: 7-18
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ForgottenTopic {
    /// Sets `topic` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 7-12
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The unique topic ID.
    ///
    /// Supported API versions: 13-18
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions indexes to forget.
    ///
    /// Supported API versions: 7-18
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
impl Encodable for ForgottenTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version >= 7 && version <= 12 {
            if version >= 12 {
                types::CompactString.encode(buf, &self.topic)?;
            } else {
                types::String.encode(buf, &self.topic)?;
            }
        }
        if version >= 13 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 7 {
            if version >= 12 {
                types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
            } else {
                types::Array(types::Int32).encode(buf, &self.partitions)?;
            }
        } else {
            if !self.partitions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
        if version >= 7 && version <= 12 {
            if version >= 12 {
                total_size += types::CompactString.compute_size(&self.topic)?;
            } else {
                total_size += types::String.compute_size(&self.topic)?;
            }
        }
        if version >= 13 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 7 {
            if version >= 12 {
                total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
            } else {
                total_size += types::Array(types::Int32).compute_size(&self.partitions)?;
            }
        } else {
            if !self.partitions.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
impl Decodable for ForgottenTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let topic = if version >= 7 && version <= 12 {
            if version >= 12 {
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
        let partitions = if version >= 7 {
            if version >= 12 {
                types::CompactArray(types::Int32).decode(buf)?
            } else {
                types::Array(types::Int32).decode(buf)?
            }
        } else {
            Default::default()
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for ForgottenTopic {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            topic_id: Uuid::nil(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ForgottenTopic {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 4-18
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    ///
    /// Supported API versions: 15-18
    pub replica_id: super::BrokerId,

    /// The epoch of this follower, or -1 if not available.
    ///
    /// Supported API versions: 15-18
    pub replica_epoch: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ReplicaState {
    /// Sets `replica_id` to the passed value.
    ///
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    ///
    /// Supported API versions: 15-18
    pub fn with_replica_id(mut self, value: super::BrokerId) -> Self {
        self.replica_id = value;
        self
    }
    /// Sets `replica_epoch` to the passed value.
    ///
    /// The epoch of this follower, or -1 if not available.
    ///
    /// Supported API versions: 15-18
    pub fn with_replica_epoch(mut self, value: i64) -> Self {
        self.replica_epoch = value;
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
impl Encodable for ReplicaState {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        if version >= 15 {
            types::Int32.encode(buf, &self.replica_id)?;
        } else {
            if self.replica_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 15 {
            types::Int64.encode(buf, &self.replica_epoch)?;
        } else {
            if self.replica_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
        if version >= 15 {
            total_size += types::Int32.compute_size(&self.replica_id)?;
        } else {
            if self.replica_id != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 15 {
            total_size += types::Int64.compute_size(&self.replica_epoch)?;
        } else {
            if self.replica_epoch != -1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 12 {
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
impl Decodable for ReplicaState {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 4 || version > 18 {
            bail!("specified version not supported by this message type");
        }
        let replica_id = if version >= 15 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let replica_epoch = if version >= 15 {
            types::Int64.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 12 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            replica_id,
            replica_epoch,
            unknown_tagged_fields,
        })
    }
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self {
            replica_id: (-1).into(),
            replica_epoch: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ReplicaState {
    const VERSIONS: VersionRange = VersionRange { min: 4, max: 18 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for FetchRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 12 {
            2
        } else {
            1
        }
    }
}
