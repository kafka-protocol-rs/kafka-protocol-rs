//! MetadataResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/MetadataResponse.json).
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

/// Valid versions: 0-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-13
    pub throttle_time_ms: i32,

    /// A list of brokers present in the cluster.
    ///
    /// Supported API versions: 0-13
    pub brokers: Vec<MetadataResponseBroker>,

    /// The cluster ID that responding broker belongs to.
    ///
    /// Supported API versions: 2-13
    pub cluster_id: Option<StrBytes>,

    /// The ID of the controller broker.
    ///
    /// Supported API versions: 1-13
    pub controller_id: super::BrokerId,

    /// Each topic in the response.
    ///
    /// Supported API versions: 0-13
    pub topics: Vec<MetadataResponseTopic>,

    /// 32-bit bitfield to represent authorized operations for this cluster.
    ///
    /// Supported API versions: 8-10
    pub cluster_authorized_operations: i32,

    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 13
    pub error_code: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 3-13
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `brokers` to the passed value.
    ///
    /// A list of brokers present in the cluster.
    ///
    /// Supported API versions: 0-13
    pub fn with_brokers(mut self, value: Vec<MetadataResponseBroker>) -> Self {
        self.brokers = value;
        self
    }
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster ID that responding broker belongs to.
    ///
    /// Supported API versions: 2-13
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `controller_id` to the passed value.
    ///
    /// The ID of the controller broker.
    ///
    /// Supported API versions: 1-13
    pub fn with_controller_id(mut self, value: super::BrokerId) -> Self {
        self.controller_id = value;
        self
    }
    /// Sets `topics` to the passed value.
    ///
    /// Each topic in the response.
    ///
    /// Supported API versions: 0-13
    pub fn with_topics(mut self, value: Vec<MetadataResponseTopic>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `cluster_authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this cluster.
    ///
    /// Supported API versions: 8-10
    pub fn with_cluster_authorized_operations(mut self, value: i32) -> Self {
        self.cluster_authorized_operations = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error.
    ///
    /// Supported API versions: 13
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
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
impl Encodable for MetadataResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.brokers)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.brokers)?;
        }
        if version >= 2 {
            if version >= 9 {
                types::CompactString.encode(buf, &self.cluster_id)?;
            } else {
                types::String.encode(buf, &self.cluster_id)?;
            }
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.controller_id)?;
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 8 && version <= 10 {
            types::Int32.encode(buf, &self.cluster_authorized_operations)?;
        } else {
            if self.cluster_authorized_operations != -2147483648 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 13 {
            types::Int16.encode(buf, &self.error_code)?;
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
        if version >= 3 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.brokers)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.brokers)?;
        }
        if version >= 2 {
            if version >= 9 {
                total_size += types::CompactString.compute_size(&self.cluster_id)?;
            } else {
                total_size += types::String.compute_size(&self.cluster_id)?;
            }
        }
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.controller_id)?;
        }
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 8 && version <= 10 {
            total_size += types::Int32.compute_size(&self.cluster_authorized_operations)?;
        } else {
            if self.cluster_authorized_operations != -2147483648 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 13 {
            total_size += types::Int16.compute_size(&self.error_code)?;
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
impl Decodable for MetadataResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = if version >= 3 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let brokers = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let cluster_id = if version >= 2 {
            if version >= 9 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            None
        };
        let controller_id = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            (-1).into()
        };
        let topics = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let cluster_authorized_operations = if version >= 8 && version <= 10 {
            types::Int32.decode(buf)?
        } else {
            -2147483648
        };
        let error_code = if version >= 13 {
            types::Int16.decode(buf)?
        } else {
            0
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
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            brokers: Default::default(),
            cluster_id: None,
            controller_id: (-1).into(),
            topics: Default::default(),
            cluster_authorized_operations: -2147483648,
            error_code: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for MetadataResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponseBroker {
    /// The broker ID.
    ///
    /// Supported API versions: 0-13
    pub node_id: super::BrokerId,

    /// The broker hostname.
    ///
    /// Supported API versions: 0-13
    pub host: StrBytes,

    /// The broker port.
    ///
    /// Supported API versions: 0-13
    pub port: i32,

    /// The rack of the broker, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 1-13
    pub rack: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataResponseBroker {
    /// Sets `node_id` to the passed value.
    ///
    /// The broker ID.
    ///
    /// Supported API versions: 0-13
    pub fn with_node_id(mut self, value: super::BrokerId) -> Self {
        self.node_id = value;
        self
    }
    /// Sets `host` to the passed value.
    ///
    /// The broker hostname.
    ///
    /// Supported API versions: 0-13
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// The broker port.
    ///
    /// Supported API versions: 0-13
    pub fn with_port(mut self, value: i32) -> Self {
        self.port = value;
        self
    }
    /// Sets `rack` to the passed value.
    ///
    /// The rack of the broker, or null if it has not been assigned to a rack.
    ///
    /// Supported API versions: 1-13
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
impl Encodable for MetadataResponseBroker {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.node_id)?;
        if version >= 9 {
            types::CompactString.encode(buf, &self.host)?;
        } else {
            types::String.encode(buf, &self.host)?;
        }
        types::Int32.encode(buf, &self.port)?;
        if version >= 1 {
            if version >= 9 {
                types::CompactString.encode(buf, &self.rack)?;
            } else {
                types::String.encode(buf, &self.rack)?;
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
        total_size += types::Int32.compute_size(&self.node_id)?;
        if version >= 9 {
            total_size += types::CompactString.compute_size(&self.host)?;
        } else {
            total_size += types::String.compute_size(&self.host)?;
        }
        total_size += types::Int32.compute_size(&self.port)?;
        if version >= 1 {
            if version >= 9 {
                total_size += types::CompactString.compute_size(&self.rack)?;
            } else {
                total_size += types::String.compute_size(&self.rack)?;
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
impl Decodable for MetadataResponseBroker {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let node_id = types::Int32.decode(buf)?;
        let host = if version >= 9 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let port = types::Int32.decode(buf)?;
        let rack = if version >= 1 {
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
            node_id,
            host,
            port,
            rack,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataResponseBroker {
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

impl Message for MetadataResponseBroker {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponsePartition {
    /// The partition error, or 0 if there was no error.
    ///
    /// Supported API versions: 0-13
    pub error_code: i16,

    /// The partition index.
    ///
    /// Supported API versions: 0-13
    pub partition_index: i32,

    /// The ID of the leader broker.
    ///
    /// Supported API versions: 0-13
    pub leader_id: super::BrokerId,

    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 7-13
    pub leader_epoch: i32,

    /// The set of all nodes that host this partition.
    ///
    /// Supported API versions: 0-13
    pub replica_nodes: Vec<super::BrokerId>,

    /// The set of nodes that are in sync with the leader for this partition.
    ///
    /// Supported API versions: 0-13
    pub isr_nodes: Vec<super::BrokerId>,

    /// The set of offline replicas of this partition.
    ///
    /// Supported API versions: 5-13
    pub offline_replicas: Vec<super::BrokerId>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataResponsePartition {
    /// Sets `error_code` to the passed value.
    ///
    /// The partition error, or 0 if there was no error.
    ///
    /// Supported API versions: 0-13
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `partition_index` to the passed value.
    ///
    /// The partition index.
    ///
    /// Supported API versions: 0-13
    pub fn with_partition_index(mut self, value: i32) -> Self {
        self.partition_index = value;
        self
    }
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the leader broker.
    ///
    /// Supported API versions: 0-13
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `leader_epoch` to the passed value.
    ///
    /// The leader epoch of this partition.
    ///
    /// Supported API versions: 7-13
    pub fn with_leader_epoch(mut self, value: i32) -> Self {
        self.leader_epoch = value;
        self
    }
    /// Sets `replica_nodes` to the passed value.
    ///
    /// The set of all nodes that host this partition.
    ///
    /// Supported API versions: 0-13
    pub fn with_replica_nodes(mut self, value: Vec<super::BrokerId>) -> Self {
        self.replica_nodes = value;
        self
    }
    /// Sets `isr_nodes` to the passed value.
    ///
    /// The set of nodes that are in sync with the leader for this partition.
    ///
    /// Supported API versions: 0-13
    pub fn with_isr_nodes(mut self, value: Vec<super::BrokerId>) -> Self {
        self.isr_nodes = value;
        self
    }
    /// Sets `offline_replicas` to the passed value.
    ///
    /// The set of offline replicas of this partition.
    ///
    /// Supported API versions: 5-13
    pub fn with_offline_replicas(mut self, value: Vec<super::BrokerId>) -> Self {
        self.offline_replicas = value;
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
impl Encodable for MetadataResponsePartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::Int32.encode(buf, &self.partition_index)?;
        types::Int32.encode(buf, &self.leader_id)?;
        if version >= 7 {
            types::Int32.encode(buf, &self.leader_epoch)?;
        }
        if version >= 9 {
            types::CompactArray(types::Int32).encode(buf, &self.replica_nodes)?;
        } else {
            types::Array(types::Int32).encode(buf, &self.replica_nodes)?;
        }
        if version >= 9 {
            types::CompactArray(types::Int32).encode(buf, &self.isr_nodes)?;
        } else {
            types::Array(types::Int32).encode(buf, &self.isr_nodes)?;
        }
        if version >= 5 {
            if version >= 9 {
                types::CompactArray(types::Int32).encode(buf, &self.offline_replicas)?;
            } else {
                types::Array(types::Int32).encode(buf, &self.offline_replicas)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int32.compute_size(&self.partition_index)?;
        total_size += types::Int32.compute_size(&self.leader_id)?;
        if version >= 7 {
            total_size += types::Int32.compute_size(&self.leader_epoch)?;
        }
        if version >= 9 {
            total_size += types::CompactArray(types::Int32).compute_size(&self.replica_nodes)?;
        } else {
            total_size += types::Array(types::Int32).compute_size(&self.replica_nodes)?;
        }
        if version >= 9 {
            total_size += types::CompactArray(types::Int32).compute_size(&self.isr_nodes)?;
        } else {
            total_size += types::Array(types::Int32).compute_size(&self.isr_nodes)?;
        }
        if version >= 5 {
            if version >= 9 {
                total_size +=
                    types::CompactArray(types::Int32).compute_size(&self.offline_replicas)?;
            } else {
                total_size += types::Array(types::Int32).compute_size(&self.offline_replicas)?;
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
impl Decodable for MetadataResponsePartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let partition_index = types::Int32.decode(buf)?;
        let leader_id = types::Int32.decode(buf)?;
        let leader_epoch = if version >= 7 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let replica_nodes = if version >= 9 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            types::Array(types::Int32).decode(buf)?
        };
        let isr_nodes = if version >= 9 {
            types::CompactArray(types::Int32).decode(buf)?
        } else {
            types::Array(types::Int32).decode(buf)?
        };
        let offline_replicas = if version >= 5 {
            if version >= 9 {
                types::CompactArray(types::Int32).decode(buf)?
            } else {
                types::Array(types::Int32).decode(buf)?
            }
        } else {
            Default::default()
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
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataResponsePartition {
    fn default() -> Self {
        Self {
            error_code: 0,
            partition_index: 0,
            leader_id: (0).into(),
            leader_epoch: -1,
            replica_nodes: Default::default(),
            isr_nodes: Default::default(),
            offline_replicas: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for MetadataResponsePartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error.
    ///
    /// Supported API versions: 0-13
    pub error_code: i16,

    /// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
    ///
    /// Supported API versions: 0-13
    pub name: Option<super::TopicName>,

    /// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
    ///
    /// Supported API versions: 10-13
    pub topic_id: Uuid,

    /// True if the topic is internal.
    ///
    /// Supported API versions: 1-13
    pub is_internal: bool,

    /// Each partition in the topic.
    ///
    /// Supported API versions: 0-13
    pub partitions: Vec<MetadataResponsePartition>,

    /// 32-bit bitfield to represent authorized operations for this topic.
    ///
    /// Supported API versions: 8-13
    pub topic_authorized_operations: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataResponseTopic {
    /// Sets `error_code` to the passed value.
    ///
    /// The topic error, or 0 if there was no error.
    ///
    /// Supported API versions: 0-13
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `name` to the passed value.
    ///
    /// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
    ///
    /// Supported API versions: 0-13
    pub fn with_name(mut self, value: Option<super::TopicName>) -> Self {
        self.name = value;
        self
    }
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
    ///
    /// Supported API versions: 10-13
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `is_internal` to the passed value.
    ///
    /// True if the topic is internal.
    ///
    /// Supported API versions: 1-13
    pub fn with_is_internal(mut self, value: bool) -> Self {
        self.is_internal = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// Each partition in the topic.
    ///
    /// Supported API versions: 0-13
    pub fn with_partitions(mut self, value: Vec<MetadataResponsePartition>) -> Self {
        self.partitions = value;
        self
    }
    /// Sets `topic_authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this topic.
    ///
    /// Supported API versions: 8-13
    pub fn with_topic_authorized_operations(mut self, value: i32) -> Self {
        self.topic_authorized_operations = value;
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
impl Encodable for MetadataResponseTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 9 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 10 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 1 {
            types::Boolean.encode(buf, &self.is_internal)?;
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.partitions)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.partitions)?;
        }
        if version >= 8 {
            types::Int32.encode(buf, &self.topic_authorized_operations)?;
        } else {
            if self.topic_authorized_operations != -2147483648 {
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 9 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 10 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 1 {
            total_size += types::Boolean.compute_size(&self.is_internal)?;
        }
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.partitions)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.partitions)?;
        }
        if version >= 8 {
            total_size += types::Int32.compute_size(&self.topic_authorized_operations)?;
        } else {
            if self.topic_authorized_operations != -2147483648 {
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
impl Decodable for MetadataResponseTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let name = if version >= 9 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let topic_id = if version >= 10 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let is_internal = if version >= 1 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let partitions = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let topic_authorized_operations = if version >= 8 {
            types::Int32.decode(buf)?
        } else {
            -2147483648
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
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataResponseTopic {
    fn default() -> Self {
        Self {
            error_code: 0,
            name: Some(Default::default()),
            topic_id: Uuid::nil(),
            is_internal: false,
            partitions: Default::default(),
            topic_authorized_operations: -2147483648,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for MetadataResponseTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for MetadataResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 9 {
            1
        } else {
            0
        }
    }
}
