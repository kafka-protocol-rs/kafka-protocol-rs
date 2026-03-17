//! JoinGroupResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/JoinGroupResponse.json).
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
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 2-9
    pub throttle_time_ms: i32,

    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-9
    pub error_code: i16,

    /// The generation ID of the group.
    ///
    /// Supported API versions: 0-9
    pub generation_id: i32,

    /// The group protocol name.
    ///
    /// Supported API versions: 7-9
    pub protocol_type: Option<StrBytes>,

    /// The group protocol selected by the coordinator.
    ///
    /// Supported API versions: 0-9
    pub protocol_name: Option<StrBytes>,

    /// The leader of the group.
    ///
    /// Supported API versions: 0-9
    pub leader: StrBytes,

    /// True if the leader must skip running the assignment.
    ///
    /// Supported API versions: 9
    pub skip_assignment: bool,

    /// The member ID assigned by the group coordinator.
    ///
    /// Supported API versions: 0-9
    pub member_id: StrBytes,

    /// The group members.
    ///
    /// Supported API versions: 0-9
    pub members: Vec<JoinGroupResponseMember>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl JoinGroupResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 2-9
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The error code, or 0 if there was no error.
    ///
    /// Supported API versions: 0-9
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `generation_id` to the passed value.
    ///
    /// The generation ID of the group.
    ///
    /// Supported API versions: 0-9
    pub fn with_generation_id(mut self, value: i32) -> Self {
        self.generation_id = value;
        self
    }
    /// Sets `protocol_type` to the passed value.
    ///
    /// The group protocol name.
    ///
    /// Supported API versions: 7-9
    pub fn with_protocol_type(mut self, value: Option<StrBytes>) -> Self {
        self.protocol_type = value;
        self
    }
    /// Sets `protocol_name` to the passed value.
    ///
    /// The group protocol selected by the coordinator.
    ///
    /// Supported API versions: 0-9
    pub fn with_protocol_name(mut self, value: Option<StrBytes>) -> Self {
        self.protocol_name = value;
        self
    }
    /// Sets `leader` to the passed value.
    ///
    /// The leader of the group.
    ///
    /// Supported API versions: 0-9
    pub fn with_leader(mut self, value: StrBytes) -> Self {
        self.leader = value;
        self
    }
    /// Sets `skip_assignment` to the passed value.
    ///
    /// True if the leader must skip running the assignment.
    ///
    /// Supported API versions: 9
    pub fn with_skip_assignment(mut self, value: bool) -> Self {
        self.skip_assignment = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID assigned by the group coordinator.
    ///
    /// Supported API versions: 0-9
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `members` to the passed value.
    ///
    /// The group members.
    ///
    /// Supported API versions: 0-9
    pub fn with_members(mut self, value: Vec<JoinGroupResponseMember>) -> Self {
        self.members = value;
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
impl Encodable for JoinGroupResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        if version >= 2 {
            types::Int32.encode(buf, &self.throttle_time_ms)?;
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::Int32.encode(buf, &self.generation_id)?;
        if version >= 7 {
            types::CompactString.encode(buf, &self.protocol_type)?;
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.protocol_name)?;
        } else {
            types::String.encode(buf, &self.protocol_name)?;
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.leader)?;
        } else {
            types::String.encode(buf, &self.leader)?;
        }
        if version >= 9 {
            types::Boolean.encode(buf, &self.skip_assignment)?;
        } else {
            if self.skip_assignment {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            types::CompactString.encode(buf, &self.member_id)?;
        } else {
            types::String.encode(buf, &self.member_id)?;
        }
        if version >= 6 {
            types::CompactArray(types::Struct { version }).encode(buf, &self.members)?;
        } else {
            types::Array(types::Struct { version }).encode(buf, &self.members)?;
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
        if version >= 2 {
            total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        }
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::Int32.compute_size(&self.generation_id)?;
        if version >= 7 {
            total_size += types::CompactString.compute_size(&self.protocol_type)?;
        }
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.protocol_name)?;
        } else {
            total_size += types::String.compute_size(&self.protocol_name)?;
        }
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.leader)?;
        } else {
            total_size += types::String.compute_size(&self.leader)?;
        }
        if version >= 9 {
            total_size += types::Boolean.compute_size(&self.skip_assignment)?;
        } else {
            if self.skip_assignment {
                bail!("A field is set that is not available on the selected protocol version");
            }
        }
        if version >= 6 {
            total_size += types::CompactString.compute_size(&self.member_id)?;
        } else {
            total_size += types::String.compute_size(&self.member_id)?;
        }
        if version >= 6 {
            total_size +=
                types::CompactArray(types::Struct { version }).compute_size(&self.members)?;
        } else {
            total_size += types::Array(types::Struct { version }).compute_size(&self.members)?;
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

#[cfg(feature = "client")]
impl Decodable for JoinGroupResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = if version >= 2 {
            types::Int32.decode(buf)?
        } else {
            0
        };
        let error_code = types::Int16.decode(buf)?;
        let generation_id = types::Int32.decode(buf)?;
        let protocol_type = if version >= 7 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let protocol_name = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let leader = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let skip_assignment = if version >= 9 {
            types::Boolean.decode(buf)?
        } else {
            false
        };
        let member_id = if version >= 6 {
            types::CompactString.decode(buf)?
        } else {
            types::String.decode(buf)?
        };
        let members = if version >= 6 {
            types::CompactArray(types::Struct { version }).decode(buf)?
        } else {
            types::Array(types::Struct { version }).decode(buf)?
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
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment,
            member_id,
            members,
            unknown_tagged_fields,
        })
    }
}

impl Default for JoinGroupResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            generation_id: -1,
            protocol_type: None,
            protocol_name: Some(Default::default()),
            leader: Default::default(),
            skip_assignment: false,
            member_id: Default::default(),
            members: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for JoinGroupResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-9
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct JoinGroupResponseMember {
    /// The group member ID.
    ///
    /// Supported API versions: 0-9
    pub member_id: StrBytes,

    /// The unique identifier of the consumer instance provided by end user.
    ///
    /// Supported API versions: 5-9
    pub group_instance_id: Option<StrBytes>,

    /// The group member metadata.
    ///
    /// Supported API versions: 0-9
    pub metadata: Bytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl JoinGroupResponseMember {
    /// Sets `member_id` to the passed value.
    ///
    /// The group member ID.
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
    /// Sets `metadata` to the passed value.
    ///
    /// The group member metadata.
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

#[cfg(feature = "broker")]
impl Encodable for JoinGroupResponseMember {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
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

#[cfg(feature = "client")]
impl Decodable for JoinGroupResponseMember {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 9 {
            bail!("specified version not supported by this message type");
        }
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
            member_id,
            group_instance_id,
            metadata,
            unknown_tagged_fields,
        })
    }
}

impl Default for JoinGroupResponseMember {
    fn default() -> Self {
        Self {
            member_id: Default::default(),
            group_instance_id: None,
            metadata: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for JoinGroupResponseMember {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 9 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for JoinGroupResponse {
    fn header_version(version: i16) -> i16 {
        if version >= 6 {
            1
        } else {
            0
        }
    }
}
