//! MetadataRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/MetadataRequest.json).
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
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    ///
    /// Supported API versions: 0-13
    pub topics: Option<Vec<MetadataRequestTopic>>,

    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    ///
    /// Supported API versions: 4-13
    pub allow_auto_topic_creation: bool,

    /// Whether to include cluster authorized operations.
    ///
    /// Supported API versions: 8-10
    pub include_cluster_authorized_operations: bool,

    /// Whether to include topic authorized operations.
    ///
    /// Supported API versions: 8-13
    pub include_topic_authorized_operations: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataRequest {
    /// Sets `topics` to the passed value.
    ///
    /// The topics to fetch metadata for.
    ///
    /// Supported API versions: 0-13
    pub fn with_topics(mut self, value: Option<Vec<MetadataRequestTopic>>) -> Self {
        self.topics = value;
        self
    }
    /// Sets `allow_auto_topic_creation` to the passed value.
    ///
    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    ///
    /// Supported API versions: 4-13
    pub fn with_allow_auto_topic_creation(mut self, value: bool) -> Self {
        self.allow_auto_topic_creation = value;
        self
    }
    /// Sets `include_cluster_authorized_operations` to the passed value.
    ///
    /// Whether to include cluster authorized operations.
    ///
    /// Supported API versions: 8-10
    pub fn with_include_cluster_authorized_operations(mut self, value: bool) -> Self {
        self.include_cluster_authorized_operations = value;
        self
    }
    /// Sets `include_topic_authorized_operations` to the passed value.
    ///
    /// Whether to include topic authorized operations.
    ///
    /// Supported API versions: 8-13
    pub fn with_include_topic_authorized_operations(mut self, value: bool) -> Self {
        self.include_topic_authorized_operations = value;
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
impl Encodable for MetadataRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 9 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.topics)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.topics)?;
        }
        if version >= 4 {
            types::Boolean.encode(buf, &self.allow_auto_topic_creation)?;
        } else {
            if !self.allow_auto_topic_creation {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 && version <= 10 {
            types::Boolean.encode(buf, &self.include_cluster_authorized_operations)?;
        } else {
            if self.include_cluster_authorized_operations {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            types::Boolean.encode(buf, &self.include_topic_authorized_operations)?;
        } else {
            if self.include_topic_authorized_operations {
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
        if version >= 9 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.topics)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.topics)?;
        }
        if version >= 4 {
            total_size += types::Boolean.compute_size(&self.allow_auto_topic_creation)?;
        } else {
            if !self.allow_auto_topic_creation {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 && version <= 10 {
            total_size +=
                types::Boolean.compute_size(&self.include_cluster_authorized_operations)?;
        } else {
            if self.include_cluster_authorized_operations {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 8 {
            total_size += types::Boolean.compute_size(&self.include_topic_authorized_operations)?;
        } else {
            if self.include_topic_authorized_operations {
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

#[cfg(feature = "broker")]
impl Decodable for MetadataRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let topics = if version >= 9 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let allow_auto_topic_creation = if version >= 4 {
            types::Boolean.decode(buf)?
        } else {
            true
        };
        let include_cluster_authorized_operations = if version >= 8 && version <= 10 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let include_topic_authorized_operations = if version >= 8 {
            types::Boolean.decode(buf)?
        } else {
            false
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
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataRequest {
    fn default() -> Self {
        Self {
            topics: Some(Default::default()),
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for MetadataRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-13
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic id.
    ///
    /// Supported API versions: 10-13
    pub topic_id: Uuid,

    /// The topic name.
    ///
    /// Supported API versions: 0-13
    pub name: Option<super::TopicName>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl MetadataRequestTopic {
    /// Sets `topic_id` to the passed value.
    ///
    /// The topic id.
    ///
    /// Supported API versions: 10-13
    pub fn with_topic_id(mut self, value: Uuid) -> Self {
        self.topic_id = value;
        self
    }
    /// Sets `name` to the passed value.
    ///
    /// The topic name.
    ///
    /// Supported API versions: 0-13
    pub fn with_name(mut self, value: Option<super::TopicName>) -> Self {
        self.name = value;
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
impl Encodable for MetadataRequestTopic {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        if version >= 10 {
            types::Uuid.encode(buf, &self.topic_id)?;
        }
        if version >= 9 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
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
        if version >= 10 {
            total_size += types::Uuid.compute_size(&self.topic_id)?;
        }
        if version >= 9 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
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
impl Decodable for MetadataRequestTopic {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            bail!("specified version not supported by this message type");
        }
        let topic_id = if version >= 10 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
        };
        let name = if version >= 9 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
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
            topic_id,
            name,
            unknown_tagged_fields,
        })
    }
}

impl Default for MetadataRequestTopic {
    fn default() -> Self {
        Self {
            topic_id: Uuid::nil(),
            name: Some(Default::default()),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for MetadataRequestTopic {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for MetadataRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 9 {
            2
        } else {
            1
        }
    }
}
