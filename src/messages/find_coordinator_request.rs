//! FindCoordinatorRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FindCoordinatorRequest.json).
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
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    ///
    /// Supported API versions: 0-3
    pub key: StrBytes,

    /// The coordinator key type. (group, transaction, share).
    ///
    /// Supported API versions: 1-6
    pub key_type: i8,

    /// The coordinator keys.
    ///
    /// Supported API versions: 4-6
    pub coordinator_keys: Vec<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FindCoordinatorRequest {
    /// Sets `key` to the passed value.
    ///
    /// The coordinator key.
    ///
    /// Supported API versions: 0-3
    pub fn with_key(mut self, value: StrBytes) -> Self {
        self.key = value;
        self
    }
    /// Sets `key_type` to the passed value.
    ///
    /// The coordinator key type. (group, transaction, share).
    ///
    /// Supported API versions: 1-6
    pub fn with_key_type(mut self, value: i8) -> Self {
        self.key_type = value;
        self
    }
    /// Sets `coordinator_keys` to the passed value.
    ///
    /// The coordinator keys.
    ///
    /// Supported API versions: 4-6
    pub fn with_coordinator_keys(mut self, value: Vec<StrBytes>) -> Self {
        self.coordinator_keys = value;
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
impl Encodable for FindCoordinatorRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        if version <= 3 {
            if version >= 3 {
                types::CompactString.encode(buf, &self.key)?;
            } else {
                types::String.encode(buf, &self.key)?;
            }
        } else {
            if !self.key.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::Int8.encode(buf, &self.key_type)?;
        } else {
            if self.key_type != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            types::CompactArray(types::CompactString).encode(buf, &self.coordinator_keys)?;
        } else {
            if !self.coordinator_keys.is_empty() {
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
        if version <= 3 {
            if version >= 3 {
                total_size += types::CompactString.compute_size(&self.key)?;
            } else {
                total_size += types::String.compute_size(&self.key)?;
            }
        } else {
            if !self.key.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::Int8.compute_size(&self.key_type)?;
        } else {
            if self.key_type != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 4 {
            total_size +=
                types::CompactArray(types::CompactString).compute_size(&self.coordinator_keys)?;
        } else {
            if !self.coordinator_keys.is_empty() {
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
impl Decodable for FindCoordinatorRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 6 {
            bail!("specified version not supported by this message type");
        }
        let key = if version <= 3 {
            if version >= 3 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            Default::default()
        };
        let key_type = if version >= 1 {
            types::Int8.decode(buf)?
        } else {
            0
        };
        let coordinator_keys = if version >= 4 {
            types::CompactArray(types::CompactString).decode(buf)?
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
            key,
            key_type,
            coordinator_keys,
            unknown_tagged_fields,
        })
    }
}

impl Default for FindCoordinatorRequest {
    fn default() -> Self {
        Self {
            key: Default::default(),
            key_type: 0,
            coordinator_keys: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FindCoordinatorRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for FindCoordinatorRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 3 {
            2
        } else {
            1
        }
    }
}
