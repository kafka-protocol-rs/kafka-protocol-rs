//! UpdateFeaturesRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/UpdateFeaturesRequest.json).
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
pub struct FeatureUpdateKey {
    /// The name of the finalized feature to be updated.
    ///
    /// Supported API versions: 0-2
    pub feature: StrBytes,

    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
    ///
    /// Supported API versions: 0-2
    pub max_version_level: i16,

    /// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
    ///
    /// Supported API versions: 0
    pub allow_downgrade: bool,

    /// Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
    ///
    /// Supported API versions: 1-2
    pub upgrade_type: i8,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FeatureUpdateKey {
    /// Sets `feature` to the passed value.
    ///
    /// The name of the finalized feature to be updated.
    ///
    /// Supported API versions: 0-2
    pub fn with_feature(mut self, value: StrBytes) -> Self {
        self.feature = value;
        self
    }
    /// Sets `max_version_level` to the passed value.
    ///
    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
    ///
    /// Supported API versions: 0-2
    pub fn with_max_version_level(mut self, value: i16) -> Self {
        self.max_version_level = value;
        self
    }
    /// Sets `allow_downgrade` to the passed value.
    ///
    /// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
    ///
    /// Supported API versions: 0
    pub fn with_allow_downgrade(mut self, value: bool) -> Self {
        self.allow_downgrade = value;
        self
    }
    /// Sets `upgrade_type` to the passed value.
    ///
    /// Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
    ///
    /// Supported API versions: 1-2
    pub fn with_upgrade_type(mut self, value: i8) -> Self {
        self.upgrade_type = value;
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
impl Encodable for FeatureUpdateKey {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.feature)?;
        types::Int16.encode(buf, &self.max_version_level)?;
        if version == 0 {
            types::Boolean.encode(buf, &self.allow_downgrade)?;
        } else {
            if self.allow_downgrade {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            types::Int8.encode(buf, &self.upgrade_type)?;
        } else {
            if self.upgrade_type != 1 {
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
        total_size += types::CompactString.compute_size(&self.feature)?;
        total_size += types::Int16.compute_size(&self.max_version_level)?;
        if version == 0 {
            total_size += types::Boolean.compute_size(&self.allow_downgrade)?;
        } else {
            if self.allow_downgrade {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 1 {
            total_size += types::Int8.compute_size(&self.upgrade_type)?;
        } else {
            if self.upgrade_type != 1 {
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
impl Decodable for FeatureUpdateKey {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let feature = types::CompactString.decode(buf)?;
        let max_version_level = types::Int16.decode(buf)?;
        let allow_downgrade = if version == 0 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let upgrade_type = if version >= 1 {
            types::Int8.decode(buf)?
        } else {
            1
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
            feature,
            max_version_level,
            allow_downgrade,
            upgrade_type,
            unknown_tagged_fields,
        })
    }
}

impl Default for FeatureUpdateKey {
    fn default() -> Self {
        Self {
            feature: Default::default(),
            max_version_level: 0,
            allow_downgrade: false,
            upgrade_type: 1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FeatureUpdateKey {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-2
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateFeaturesRequest {
    /// How long to wait in milliseconds before timing out the request.
    ///
    /// Supported API versions: 0-2
    pub timeout_ms: i32,

    /// The list of updates to finalized features.
    ///
    /// Supported API versions: 0-2
    pub feature_updates: Vec<FeatureUpdateKey>,

    /// True if we should validate the request, but not perform the upgrade or downgrade.
    ///
    /// Supported API versions: 1-2
    pub validate_only: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl UpdateFeaturesRequest {
    /// Sets `timeout_ms` to the passed value.
    ///
    /// How long to wait in milliseconds before timing out the request.
    ///
    /// Supported API versions: 0-2
    pub fn with_timeout_ms(mut self, value: i32) -> Self {
        self.timeout_ms = value;
        self
    }
    /// Sets `feature_updates` to the passed value.
    ///
    /// The list of updates to finalized features.
    ///
    /// Supported API versions: 0-2
    pub fn with_feature_updates(mut self, value: Vec<FeatureUpdateKey>) -> Self {
        self.feature_updates = value;
        self
    }
    /// Sets `validate_only` to the passed value.
    ///
    /// True if we should validate the request, but not perform the upgrade or downgrade.
    ///
    /// Supported API versions: 1-2
    pub fn with_validate_only(mut self, value: bool) -> Self {
        self.validate_only = value;
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
impl Encodable for UpdateFeaturesRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.timeout_ms)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.feature_updates)?;
        if version >= 1 {
            types::Boolean.encode(buf, &self.validate_only)?;
        } else {
            if self.validate_only {
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
        total_size += types::Int32.compute_size(&self.timeout_ms)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.feature_updates)?;
        if version >= 1 {
            total_size += types::Boolean.compute_size(&self.validate_only)?;
        } else {
            if self.validate_only {
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
impl Decodable for UpdateFeaturesRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 2 {
            bail!("specified version not supported by this message type");
        }
        let timeout_ms = types::Int32.decode(buf)?;
        let feature_updates = types::CompactArray(types::Struct { version }).decode(buf)?;
        let validate_only = if version >= 1 {
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
            timeout_ms,
            feature_updates,
            validate_only,
            unknown_tagged_fields,
        })
    }
}

impl Default for UpdateFeaturesRequest {
    fn default() -> Self {
        Self {
            timeout_ms: 60000,
            feature_updates: Default::default(),
            validate_only: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for UpdateFeaturesRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 2 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for UpdateFeaturesRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
