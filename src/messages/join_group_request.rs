//! JoinGroupRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/JoinGroupRequest.json).
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

/// Valid versions: 0-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct JoinGroupRequest {
    /// The group identifier.
    ///
    /// Supported API versions: 0-9
    pub group_id: super::GroupId,

    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    ///
    /// Supported API versions: 0-9
    pub session_timeout_ms: i32,

    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    ///
    /// Supported API versions: 1-9
    pub rebalance_timeout_ms: i32,

    /// The member id assigned by the group coordinator.
    ///
    /// Supported API versions: 0-9
    pub member_id: StrBytes,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Supported API versions: 5-9
    pub group_instance_id: Option<StrBytes>,

    /// The unique name the for class of protocols implemented by the group we want to join.
    ///
    /// Supported API versions: 0-9
    pub protocol_type: StrBytes,

    /// The list of protocols that the member supports.
    ///
    /// Supported API versions: 0-9
    pub protocols: Vec<JoinGroupRequestProtocol>,

    /// The reason why the member (re-)joins the group.
    ///
    /// Supported API versions: 8-9
    pub reason: Option<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl JoinGroupRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The group identifier.
    ///
    /// Supported API versions: 0-9
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `session_timeout_ms` to the passed value.
    ///
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    ///
    /// Supported API versions: 0-9
    pub fn with_session_timeout_ms(mut self, value: i32) -> Self {
        self.session_timeout_ms = value;
        self
    }
    /// Sets `rebalance_timeout_ms` to the passed value.
    ///
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    ///
    /// Supported API versions: 1-9
    pub fn with_rebalance_timeout_ms(mut self, value: i32) -> Self {
        self.rebalance_timeout_ms = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member id assigned by the group coordinator.
    ///
    /// Supported API versions: 0-9
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `group_instance_id` to the passed value.
    ///
    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Supported API versions: 5-9
    pub fn with_group_instance_id(mut self, value: Option<StrBytes>) -> Self {
        self.group_instance_id = value;
        self
    }
    /// Sets `protocol_type` to the passed value.
    ///
    /// The unique name the for class of protocols implemented by the group we want to join.
    ///
    /// Supported API versions: 0-9
    pub fn with_protocol_type(mut self, value: StrBytes) -> Self {
        self.protocol_type = value;
        self
    }
    /// Sets `protocols` to the passed value.
    ///
    /// The list of protocols that the member supports.
    ///
    /// Supported API versions: 0-9
    pub fn with_protocols(mut self, value: Vec<JoinGroupRequestProtocol>) -> Self {
        self.protocols = value;
        self
    }
    /// Sets `reason` to the passed value.
    ///
    /// The reason why the member (re-)joins the group.
    ///
    /// Supported API versions: 8-9
    pub fn with_reason(mut self, value: Option<StrBytes>) -> Self {
        self.reason = value;
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
impl Encodable for JoinGroupRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.group_id)?;
        } else {
            types::String.encode(buf, &self.group_id)?;
        }
        types::Int32.encode(buf, &self.session_timeout_ms)?;
        if version >= 1 {
            types::Int32.encode(buf, &self.rebalance_timeout_ms)?;
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.member_id)?;
        } else {
            types::String.encode(buf, &self.member_id)?;
        }
        if version >= 5 {
            if version >= 6 {
                types::CompactString.encode(buf, &self.group_instance_id)?;
            } else {
                types::String.encode(buf, &self.group_instance_id)?;
            }
        } else {
            if !self.group_instance_id.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.protocol_type)?;
        } else {
            types::String.encode(buf, &self.protocol_type)?;
        }
        if version >= 6 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.protocols)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.protocols)?;
        }
        if version >= 8 {
            types::CompactString.encode(buf, &self.reason)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.group_id)?;
        } else {
            total_size += types::String.compute_size(&self.group_id)?;
        }
        total_size += types::Int32.compute_size(&self.session_timeout_ms)?;
        if version >= 1 {
            total_size += types::Int32.compute_size(&self.rebalance_timeout_ms)?;
        }
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.member_id)?;
        } else {
            total_size += types::String.compute_size(&self.member_id)?;
        }
        if version >= 5 {
            if version >= 6 {
                total_size += types::CompactString.compute_size(&self.group_instance_id)?;
            } else {
                total_size += types::String.compute_size(&self.group_instance_id)?;
            }
        } else {
            if !self.group_instance_id.is_none() {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.protocol_type)?;
        } else {
            total_size += types::String.compute_size(&self.protocol_type)?;
        }
        if version >= 6 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.protocols)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.protocols)?;
        }
        if version >= 8 {
            total_size += types::CompactString.compute_size(&self.reason)?;
        }
        if version >= 6 {
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
impl Decodable for JoinGroupRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let group_id = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let session_timeout_ms = types::Int32.decode(buf)?;
        let rebalance_timeout_ms = if version >= 1 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let member_id = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let group_instance_id = if version >= 5 {
            if version >= 6 {
                types::CompactString.decode(buf)?
            } else {
                types::String.decode(buf)?
            }
        } else {
            None
        };
        let protocol_type = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let protocols = if version >= 6 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
        };
        let reason = if version >= 8 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
            unknown_tagged_fields,
        })
    }
}

impl Default for JoinGroupRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            session_timeout_ms: 0,
            rebalance_timeout_ms: -1,
            member_id: Default::default(),
            group_instance_id: None,
            protocol_type: Default::default(),
            protocols: Default::default(),
            reason: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for JoinGroupRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct JoinGroupRequestProtocol {
    /// The protocol name.
    ///
    /// Supported API versions: 0-9
    pub name: StrBytes,

    /// The protocol metadata.
    ///
    /// Supported API versions: 0-9
    pub metadata: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl JoinGroupRequestProtocol {
    /// Sets `name` to the passed value.
    ///
    /// The protocol name.
    ///
    /// Supported API versions: 0-9
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `metadata` to the passed value.
    ///
    /// The protocol metadata.
    ///
    /// Supported API versions: 0-9
    pub fn with_metadata(mut self, value: Bytes) -> Self {
        self.metadata = value;
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
impl Encodable for JoinGroupRequestProtocol {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.name)?;
        } else {
            types::String.encode(buf, &self.name)?;
        }
        if version >= 6 {
            types::CompactBytes.encode(buf, &self.metadata)?;
        } else {
            types::Bytes.encode(buf, &self.metadata)?;
        }
        if version >= 6 {
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
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.name)?;
        } else {
            total_size += types::String.compute_size(&self.name)?;
        }
        if version >= 6 {
            total_size += types::CompactBytes.compute_size(&self.metadata)?;
        } else {
            total_size += types::Bytes.compute_size(&self.metadata)?;
        }
        if version >= 6 {
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
impl Decodable for JoinGroupRequestProtocol {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let name = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let metadata = if version >= 6 {
            types::CompactBytes.decode(buf)?
        } else {
            types::Bytes.decode(buf)?
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 6 {
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
            metadata,
            unknown_tagged_fields,
        })
    }
}

impl Default for JoinGroupRequestProtocol {
    fn default() -> Self {
        Self {
            name: Default::default(),
            metadata: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for JoinGroupRequestProtocol {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for JoinGroupRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 6 {
            2
        } else {
            1
        }
    }
}
