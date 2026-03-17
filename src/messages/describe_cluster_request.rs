//! DescribeClusterRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeClusterRequest.json).
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
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    ///
    /// Supported API versions: 0-2
    pub include_cluster_authorized_operations: bool,

    /// The endpoint type to describe. 1=brokers, 2=controllers.
    ///
    /// Supported API versions: 1-2
    pub endpoint_type: i8,

    /// Whether to include fenced brokers when listing brokers.
    ///
    /// Supported API versions: 2
    pub include_fenced_brokers: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribeClusterRequest {
    /// Sets `include_cluster_authorized_operations` to the passed value.
    ///
    /// Whether to include cluster authorized operations.
    ///
    /// Supported API versions: 0-2
    pub fn with_include_cluster_authorized_operations(mut self, value: bool) -> Self {
        self.include_cluster_authorized_operations = value;
        self
    }
    /// Sets `endpoint_type` to the passed value.
    ///
    /// The endpoint type to describe. 1=brokers, 2=controllers.
    ///
    /// Supported API versions: 1-2
    pub fn with_endpoint_type(mut self, value: i8) -> Self {
        self.endpoint_type = value;
        self
    }
    /// Sets `include_fenced_brokers` to the passed value.
    ///
    /// Whether to include fenced brokers when listing brokers.
    ///
    /// Supported API versions: 2
    pub fn with_include_fenced_brokers(mut self, value: bool) -> Self {
        self.include_fenced_brokers = value;
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
impl Encodable for DescribeClusterRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Boolean.encode(buf, &self.include_cluster_authorized_operations)?;
        if version >= 1 {
            types::Int8.encode(buf, &self.endpoint_type)?;
        } else {
            if self.endpoint_type != 1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            types::Boolean.encode(buf, &self.include_fenced_brokers)?;
        } else {
            if self.include_fenced_brokers {
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
        total_size += types::Boolean.compute_size(&self.include_cluster_authorized_operations)?;
        if version >= 1 {
            total_size += types::Int8.compute_size(&self.endpoint_type)?;
        } else {
            if self.endpoint_type != 1 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 2 {
            total_size += types::Boolean.compute_size(&self.include_fenced_brokers)?;
        } else {
            if self.include_fenced_brokers {
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
impl Decodable for DescribeClusterRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let include_cluster_authorized_operations = types::Boolean.decode(buf)?;
        let endpoint_type = if version >= 1 {
            types::Int8.decode(buf)?
        } else {
            1
        };
        let include_fenced_brokers = if version >= 2 {
            types::Boolean.decode(buf)?
        } else {
            false
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
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribeClusterRequest {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: 1,
            include_fenced_brokers: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribeClusterRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for DescribeClusterRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
