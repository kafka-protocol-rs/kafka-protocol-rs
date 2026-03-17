//! ApiVersionsResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json).
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

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersion {
    /// The API index.
    ///
    /// Supported API versions: 0-4
    pub api_key: i16,

    /// The minimum supported version, inclusive.
    ///
    /// Supported API versions: 0-4
    pub min_version: i16,

    /// The maximum supported version, inclusive.
    ///
    /// Supported API versions: 0-4
    pub max_version: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ApiVersion {
    /// Sets `api_key` to the passed value.
    ///
    /// The API index.
    ///
    /// Supported API versions: 0-4
    pub fn with_api_key(mut self, value: i16) -> Self {
        self.api_key = value;
        self
    }
    /// Sets `min_version` to the passed value.
    ///
    /// The minimum supported version, inclusive.
    ///
    /// Supported API versions: 0-4
    pub fn with_min_version(mut self, value: i16) -> Self {
        self.min_version = value;
        self
    }
    /// Sets `max_version` to the passed value.
    ///
    /// The maximum supported version, inclusive.
    ///
    /// Supported API versions: 0-4
    pub fn with_max_version(mut self, value: i16) -> Self {
        self.max_version = value;
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
impl Encodable for ApiVersion {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.api_key)?;
        types::Int16.encode(buf, &self.min_version)?;
        types::Int16.encode(buf, &self.max_version)?;
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
        total_size += types::Int16.compute_size(&self.api_key)?;
        total_size += types::Int16.compute_size(&self.min_version)?;
        total_size += types::Int16.compute_size(&self.max_version)?;
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

#[cfg(feature = "client")]
impl Decodable for ApiVersion {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let api_key = types::Int16.decode(buf)?;
        let min_version = types::Int16.decode(buf)?;
        let max_version = types::Int16.decode(buf)?;
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
            api_key,
            min_version,
            max_version,
            unknown_tagged_fields,
        })
    }
}

impl Default for ApiVersion {
    fn default() -> Self {
        Self {
            api_key: 0,
            min_version: 0,
            max_version: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ApiVersion {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    ///
    /// Supported API versions: 0-4
    pub error_code: i16,

    /// The APIs supported by the broker.
    ///
    /// Supported API versions: 0-4
    pub api_keys: Vec<ApiVersion>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-4
    pub throttle_time_ms: i32,

    /// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    ///
    /// Supported API versions: 3-4
    pub supported_features: Vec<SupportedFeatureKey>,

    /// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    ///
    /// Supported API versions: 3-4
    pub finalized_features_epoch: i64,

    /// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    ///
    /// Supported API versions: 3-4
    pub finalized_features: Vec<FinalizedFeatureKey>,

    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    ///
    /// Supported API versions: 3-4
    pub zk_migration_ready: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ApiVersionsResponse {
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code.
    ///
    /// Supported API versions: 0-4
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `api_keys` to the passed value.
    ///
    /// The APIs supported by the broker.
    ///
    /// Supported API versions: 0-4
    pub fn with_api_keys(mut self, value: Vec<ApiVersion>) -> Self {
        self.api_keys = value;
        self
    }
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 1-4
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `supported_features` to the passed value.
    ///
    /// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    ///
    /// Supported API versions: 3-4
    pub fn with_supported_features(mut self, value: Vec<SupportedFeatureKey>) -> Self {
        self.supported_features = value;
        self
    }
    /// Sets `finalized_features_epoch` to the passed value.
    ///
    /// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    ///
    /// Supported API versions: 3-4
    pub fn with_finalized_features_epoch(mut self, value: i64) -> Self {
        self.finalized_features_epoch = value;
        self
    }
    /// Sets `finalized_features` to the passed value.
    ///
    /// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    ///
    /// Supported API versions: 3-4
    pub fn with_finalized_features(mut self, value: Vec<FinalizedFeatureKey>) -> Self {
        self.finalized_features = value;
        self
    }
    /// Sets `zk_migration_ready` to the passed value.
    ///
    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    ///
    /// Supported API versions: 3-4
    pub fn with_zk_migration_ready(mut self, value: bool) -> Self {
        self.zk_migration_ready = value;
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
impl Encodable for ApiVersionsResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        if version >= 3 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.api_keys)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.api_keys)?;
        }
        if version >= 1 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        if version >= 3 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if !self.supported_features.is_empty() {
                num_tagged_fields += 1;
            }
            if self.finalized_features_epoch != -1 {
                num_tagged_fields += 1;
            }
            if !self.finalized_features.is_empty() {
                num_tagged_fields += 1;
            }
            if self.zk_migration_ready {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;
            if !self.supported_features.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.supported_features)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 0)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::CompactArray(types::Struct { version })
                    .encode(buf, &self.supported_features)?;
            }
            if self.finalized_features_epoch != -1 {
                let computed_size = types::Int64.compute_size(&self.finalized_features_epoch)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 1)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Int64.encode(buf, &self.finalized_features_epoch)?;
            }
            if !self.finalized_features.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.finalized_features)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 2)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::CompactArray(types::Struct { version })
                    .encode(buf, &self.finalized_features)?;
            }
            if self.zk_migration_ready {
                let computed_size = types::Boolean.compute_size(&self.zk_migration_ready)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                types::UnsignedVarInt.encode(buf, 3)?;
                types::UnsignedVarInt.encode(buf, computed_size as u32)?;
                types::Boolean.encode(buf, &self.zk_migration_ready)?;
            }

            write_unknown_tagged_fields(buf, 4.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int16.compute_size(&self.error_code)?;
        if version >= 3 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.api_keys)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.api_keys)?;
        }
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        if version >= 3 {
            let mut num_tagged_fields = self.unknown_tagged_fields.len();
            if !self.supported_features.is_empty() {
                num_tagged_fields += 1;
            }
            if self.finalized_features_epoch != -1 {
                num_tagged_fields += 1;
            }
            if !self.finalized_features.is_empty() {
                num_tagged_fields += 1;
            }
            if self.zk_migration_ready {
                num_tagged_fields += 1;
            }
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;
            if !self.supported_features.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.supported_features)?;
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
            if self.finalized_features_epoch != -1 {
                let computed_size = types::Int64.compute_size(&self.finalized_features_epoch)?;
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
            if !self.finalized_features.is_empty() {
                let computed_size = types::CompactArray(types::Struct { version })
                    .compute_size(&self.finalized_features)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(2)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }
            if self.zk_migration_ready {
                let computed_size = types::Boolean.compute_size(&self.zk_migration_ready)?;
                if computed_size > std::u32::MAX as usize {
                    bail!(
                        "Tagged field is too large to encode ({} bytes)",
                        computed_size
                    );
                }
                total_size += types::UnsignedVarInt.compute_size(3)?;
                total_size += types::UnsignedVarInt.compute_size(computed_size as u32)?;
                total_size += computed_size;
            }

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for ApiVersionsResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let api_keys = if version >= 3 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let throttle_time_ms = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let mut supported_features = Default::default();
        let mut finalized_features_epoch = -1;
        let mut finalized_features = Default::default();
        let mut zk_migration_ready = false;
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                match tag {
                    0 => {
                        supported_features =
                            types::CompactArray(types::Struct { version }).decode(buf)?;
                    }
                    1 => {
                        finalized_features_epoch = types::Int64.decode(buf)?;
                    }
                    2 => {
                        finalized_features =
                            types::CompactArray(types::Struct { version }).decode(buf)?;
                    }
                    3 => {
                        zk_migration_ready = types::Boolean.decode(buf)?;
                    }
                    _ => {
                        let unknown_value = buf.try_get_bytes(size as usize)?;
                        unknown_tagged_fields.insert(tag as i32, unknown_value);
                    }
                }
            }
        }
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
            unknown_tagged_fields,
        })
    }
}

impl Default for ApiVersionsResponse {
    fn default() -> Self {
        Self {
            error_code: 0,
            api_keys: Default::default(),
            throttle_time_ms: 0,
            supported_features: Default::default(),
            finalized_features_epoch: -1,
            finalized_features: Default::default(),
            zk_migration_ready: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ApiVersionsResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct FinalizedFeatureKey {
    /// The name of the feature.
    ///
    /// Supported API versions: 3-4
    pub name: StrBytes,

    /// The cluster-wide finalized max version level for the feature.
    ///
    /// Supported API versions: 3-4
    pub max_version_level: i16,

    /// The cluster-wide finalized min version level for the feature.
    ///
    /// Supported API versions: 3-4
    pub min_version_level: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl FinalizedFeatureKey {
    /// Sets `name` to the passed value.
    ///
    /// The name of the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `max_version_level` to the passed value.
    ///
    /// The cluster-wide finalized max version level for the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_max_version_level(mut self, value: i16) -> Self {
        self.max_version_level = value;
        self
    }
    /// Sets `min_version_level` to the passed value.
    ///
    /// The cluster-wide finalized min version level for the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_min_version_level(mut self, value: i16) -> Self {
        self.min_version_level = value;
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
impl Encodable for FinalizedFeatureKey {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int16.encode(buf, &self.max_version_level)?;
        } else {
            if self.max_version_level != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int16.encode(buf, &self.min_version_level)?;
        } else {
            if self.min_version_level != 0 {
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
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int16.compute_size(&self.max_version_level)?;
        } else {
            if self.max_version_level != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int16.compute_size(&self.min_version_level)?;
        } else {
            if self.min_version_level != 0 {
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

#[cfg(feature = "client")]
impl Decodable for FinalizedFeatureKey {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let max_version_level = if version >= 3 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let min_version_level = if version >= 3 {
            types::Int16.decode(buf)?
        } else {
            0
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
            name,
            max_version_level,
            min_version_level,
            unknown_tagged_fields,
        })
    }
}

impl Default for FinalizedFeatureKey {
    fn default() -> Self {
        Self {
            name: Default::default(),
            max_version_level: 0,
            min_version_level: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for FinalizedFeatureKey {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-4
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SupportedFeatureKey {
    /// The name of the feature.
    ///
    /// Supported API versions: 3-4
    pub name: StrBytes,

    /// The minimum supported version for the feature.
    ///
    /// Supported API versions: 3-4
    pub min_version: i16,

    /// The maximum supported version for the feature.
    ///
    /// Supported API versions: 3-4
    pub max_version: i16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl SupportedFeatureKey {
    /// Sets `name` to the passed value.
    ///
    /// The name of the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `min_version` to the passed value.
    ///
    /// The minimum supported version for the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_min_version(mut self, value: i16) -> Self {
        self.min_version = value;
        self
    }
    /// Sets `max_version` to the passed value.
    ///
    /// The maximum supported version for the feature.
    ///
    /// Supported API versions: 3-4
    pub fn with_max_version(mut self, value: i16) -> Self {
        self.max_version = value;
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
impl Encodable for SupportedFeatureKey {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int16.encode(buf, &self.min_version)?;
        } else {
            if self.min_version != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            types::Int16.encode(buf, &self.max_version)?;
        } else {
            if self.max_version != 0 {
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
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            if !self.name.is_empty() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int16.compute_size(&self.min_version)?;
        } else {
            if self.min_version != 0 {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 3 {
            total_size += types::Int16.compute_size(&self.max_version)?;
        } else {
            if self.max_version != 0 {
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

#[cfg(feature = "client")]
impl Decodable for SupportedFeatureKey {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 4 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let min_version = if version >= 3 {
            types::Int16.decode(buf)?
        } else {
            0
        };
        let max_version = if version >= 3 {
            types::Int16.decode(buf)?
        } else {
            0
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
            name,
            min_version,
            max_version,
            unknown_tagged_fields,
        })
    }
}

impl Default for SupportedFeatureKey {
    fn default() -> Self {
        Self {
            name: Default::default(),
            min_version: 0,
            max_version: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for SupportedFeatureKey {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ApiVersionsResponse {
    fn header_version(version: i16) -> i16 {
        0
    }
}
