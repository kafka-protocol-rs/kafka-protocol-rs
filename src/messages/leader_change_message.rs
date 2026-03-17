//! LeaderChangeMessage
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/LeaderChangeMessage.json).
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

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderChangeMessage {
    /// The version of the leader change message.
    ///
    /// Supported API versions: 0-1
    pub version: i16,

    /// The ID of the newly elected leader.
    ///
    /// Supported API versions: 0-1
    pub leader_id: super::BrokerId,

    /// The set of voters in the quorum for this epoch.
    ///
    /// Supported API versions: 0-1
    pub voters: Vec<Voter>,

    /// The voters who voted for the leader at the time of election.
    ///
    /// Supported API versions: 0-1
    pub granting_voters: Vec<Voter>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl LeaderChangeMessage {
    /// Sets `version` to the passed value.
    ///
    /// The version of the leader change message.
    ///
    /// Supported API versions: 0-1
    pub fn with_version(mut self, value: i16) -> Self {
        self.version = value;
        self
    }
    /// Sets `leader_id` to the passed value.
    ///
    /// The ID of the newly elected leader.
    ///
    /// Supported API versions: 0-1
    pub fn with_leader_id(mut self, value: super::BrokerId) -> Self {
        self.leader_id = value;
        self
    }
    /// Sets `voters` to the passed value.
    ///
    /// The set of voters in the quorum for this epoch.
    ///
    /// Supported API versions: 0-1
    pub fn with_voters(mut self, value: Vec<Voter>) -> Self {
        self.voters = value;
        self
    }
    /// Sets `granting_voters` to the passed value.
    ///
    /// The voters who voted for the leader at the time of election.
    ///
    /// Supported API versions: 0-1
    pub fn with_granting_voters(mut self, value: Vec<Voter>) -> Self {
        self.granting_voters = value;
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

impl Encodable for LeaderChangeMessage {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.version)?;
        types::Int32.encode(buf, &self.leader_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.voters)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.granting_voters)?;
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
        total_size += types::Int32.compute_size(&self.leader_id)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.voters)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.granting_voters)?;
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

impl Decodable for LeaderChangeMessage {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let version = types::Int16.decode(buf)?;
        let leader_id = types::Int32.decode(buf)?;
        let voters = types::CompactArray(types::Struct { version }).decode(buf)?;
        let granting_voters = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            leader_id,
            voters,
            granting_voters,
            unknown_tagged_fields,
        })
    }
}

impl Default for LeaderChangeMessage {
    fn default() -> Self {
        Self {
            version: 0,
            leader_id: (0).into(),
            voters: Default::default(),
            granting_voters: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for LeaderChangeMessage {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0-1
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Voter {
    /// The ID of the voter.
    ///
    /// Supported API versions: 0-1
    pub voter_id: i32,

    /// The directory id of the voter.
    ///
    /// Supported API versions: 1
    pub voter_directory_id: Uuid,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Voter {
    /// Sets `voter_id` to the passed value.
    ///
    /// The ID of the voter.
    ///
    /// Supported API versions: 0-1
    pub fn with_voter_id(mut self, value: i32) -> Self {
        self.voter_id = value;
        self
    }
    /// Sets `voter_directory_id` to the passed value.
    ///
    /// The directory id of the voter.
    ///
    /// Supported API versions: 1
    pub fn with_voter_directory_id(mut self, value: Uuid) -> Self {
        self.voter_directory_id = value;
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

impl Encodable for Voter {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.voter_id)?;
        if version >= 1 {
            types::Uuid.encode(buf, &self.voter_directory_id)?;
        } else {
            if &self.voter_directory_id != &Uuid::nil() {
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
        total_size += types::Int32.compute_size(&self.voter_id)?;
        if version >= 1 {
            total_size += types::Uuid.compute_size(&self.voter_directory_id)?;
        } else {
            if &self.voter_directory_id != &Uuid::nil() {
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

impl Decodable for Voter {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 1 {
            bail!("specified version not supported by this message type");
        }
        let voter_id = types::Int32.decode(buf)?;
        let voter_directory_id = if version >= 1 {
            types::Uuid.decode(buf)?
        } else {
            Uuid::nil()
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
            voter_id,
            voter_directory_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for Voter {
    fn default() -> Self {
        Self {
            voter_id: 0,
            voter_directory_id: Uuid::nil(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Voter {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 1 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
