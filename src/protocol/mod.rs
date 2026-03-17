//! Most types are used internally in encoding/decoding, and are not required by typical use cases
//! for interacting with the protocol. However, types can be used for decoding partial messages,
//! or rewriting parts of an encoded message.
use std::cmp;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::{borrow::Borrow, fmt::Display};

use crate::error::{bail, Result};
use buf::{ByteBuf, ByteBufMut};
use bytes::Bytes;

pub mod buf;
pub mod types;

pub use buf::SegmentedBuf;

mod str_bytes {
    use bytes::Bytes;
    use std::borrow::Borrow;
    use std::convert::TryFrom;
    use std::fmt::{Debug, Display, Formatter};
    use std::ops::Deref;
    use std::str::Utf8Error;

    /// A string type backed by [Bytes].
    #[derive(Clone, Hash, Ord, PartialOrd, PartialEq, Eq, Default)]
    pub struct StrBytes(Bytes);

    impl StrBytes {
        /// Creates a new empty `StrBytes`.
        ///
        /// This will not allocate and the returned `StrBytes` handle will be empty.
        pub const fn new() -> Self {
            Self(Bytes::new())
        }

        /// Construct a [StrBytes] from the given [Bytes] instance,
        /// checking that it contains valid UTF-8 data.
        pub fn from_utf8(bytes: Bytes) -> Result<Self, Utf8Error> {
            let _: &str = std::str::from_utf8(&bytes)?;
            Ok(Self(bytes))
        }

        /// Construct a [StrBytes] from the provided static [str].
        pub const fn from_static_str(s: &'static str) -> Self {
            Self(Bytes::from_static(s.as_bytes()))
        }

        /// Construct a [StrBytes] from the provided [String] without additional allocations.
        pub fn from_string(s: String) -> Self {
            Self(Bytes::from(s.into_bytes()))
        }

        /// View the contents of this [StrBytes] as a [str] reference.
        pub fn as_str(&self) -> &str {
            // SAFETY: all methods of constructing `self` check that the backing data is valid utf8,
            // and bytes::Bytes guarantees that its contents will not change unless we mutate it,
            // and we never mutate it.
            unsafe { std::str::from_utf8_unchecked(&self.0) }
        }

        /// Extract the underlying [Bytes].
        pub fn into_bytes(self) -> Bytes {
            self.0
        }
    }

    impl TryFrom<Bytes> for StrBytes {
        type Error = Utf8Error;

        fn try_from(value: Bytes) -> Result<Self, Self::Error> {
            StrBytes::from_utf8(value)
        }
    }

    impl From<StrBytes> for Bytes {
        fn from(value: StrBytes) -> Bytes {
            value.0
        }
    }

    impl From<String> for StrBytes {
        fn from(value: String) -> Self {
            Self::from_string(value)
        }
    }

    impl From<&'static str> for StrBytes {
        fn from(value: &'static str) -> Self {
            Self::from_static_str(value)
        }
    }

    impl Deref for StrBytes {
        type Target = str;

        fn deref(&self) -> &Self::Target {
            self.as_str()
        }
    }

    impl Debug for StrBytes {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(self.as_str(), f)
        }
    }

    impl Display for StrBytes {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&**self, f)
        }
    }

    impl PartialEq<str> for StrBytes {
        fn eq(&self, other: &str) -> bool {
            self.as_str().eq(other)
        }
    }

    impl Borrow<[u8]> for StrBytes {
        fn borrow(&self) -> &[u8] {
            // Note that there is an equivalent Hash implementation between
            // &[u8] and StrBytes, which makes this impl correct
            // as described in the `std::borrow::Borrow` docs.
            self.as_bytes()
        }
    }
}

pub use str_bytes::StrBytes;

use crate::messages::{ApiKey, RequestHeader};

pub(crate) trait NewType<Inner>: From<Inner> + Into<Inner> + Borrow<Inner> {}

impl<T> NewType<T> for T {}

pub(crate) trait Encoder<Value> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Value) -> Result<()>;
    fn compute_size(&self, value: Value) -> Result<usize>;
    fn fixed_size(&self) -> Option<usize> {
        None
    }
}

pub(crate) trait Decoder<Value> {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Value>;
}

/// The range of versions (min, max) allowed for agiven message.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct VersionRange {
    /// The minimum version in the range.
    pub min: i16,
    /// The maximum version in the range.
    pub max: i16,
}

impl VersionRange {
    /// Checks whether the version range contains no versions.
    pub fn is_empty(&self) -> bool {
        self.min > self.max
    }

    /// Finds the valid intersection with a provided other version range.
    pub fn intersect(&self, other: &VersionRange) -> VersionRange {
        VersionRange {
            min: cmp::max(self.min, other.min),
            max: cmp::min(self.max, other.max),
        }
    }
}

impl Display for VersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.min, self.max)
    }
}

/// An API request or response.
///
/// All API messages must provide a set of valid versions.
pub trait Message: Sized {
    /// The valid versions for this message.
    const VERSIONS: VersionRange;
    /// The deprecated versions for this message.
    const DEPRECATED_VERSIONS: Option<VersionRange>;
}

/// An encodable message.
pub trait Encodable: Sized {
    /// Encode the message into the target buffer.
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()>;
    /// Compute the total size of the message when encoded.
    fn compute_size(&self, version: i16) -> Result<usize>;
}

/// A decodable message.
pub trait Decodable: Sized {
    /// Decode the message from the provided buffer and version.
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self>;
}

/// Every message has a set of versions valid for a given header version.
pub trait HeaderVersion {
    /// Maps a header version to a given version for a particular API message.
    fn header_version(version: i16) -> i16;
}

/// An API request.
///
/// Every abstract request must be able to provide the following items:
/// - An API key mapped to this request.
/// - A version based on a provided header version.
pub trait Request: Message + Encodable + Decodable + HeaderVersion {
    /// The API key of this request.
    const KEY: i16;
    /// The response associated with this request.
    type Response: Message + Encodable + Decodable + HeaderVersion;
}

/// Decode the request header from the provided buffer.
pub fn decode_request_header_from_buffer<B: ByteBuf>(buf: &mut B) -> Result<RequestHeader> {
    let api_key = ApiKey::try_from(bytes::Buf::get_i16(&mut buf.peek_bytes(0..2)))
        .map_err(|_| crate::error::ProtocolError::UnknownApiKey)?;
    let api_version = bytes::Buf::get_i16(&mut buf.peek_bytes(2..4));
    let header_version = api_key.request_header_version(api_version);
    RequestHeader::decode(buf, header_version)
}

/// Encode the request header into the provided buffer.
pub fn encode_request_header_into_buffer<B: ByteBufMut>(
    buf: &mut B,
    header: &RequestHeader,
) -> Result<()> {
    let api_key = ApiKey::try_from(header.request_api_key)
        .map_err(|_| crate::error::ProtocolError::UnknownApiKey)?;
    let version = api_key.request_header_version(header.request_api_version);
    header.encode(buf, version)
}

pub(crate) fn write_unknown_tagged_fields<B: ByteBufMut, R: RangeBounds<i32>>(
    buf: &mut B,
    range: R,
    unknown_tagged_fields: &BTreeMap<i32, Bytes>,
) -> Result<()> {
    for (&k, v) in unknown_tagged_fields.range(range) {
        if v.len() > u32::MAX as usize {
            bail!("Tagged field is too long to encode ({} bytes)", v.len());
        }
        types::UnsignedVarInt.encode(buf, k as u32)?;
        types::UnsignedVarInt.encode(buf, v.len() as u32)?;
        buf.put_slice(v);
    }
    Ok(())
}

pub(crate) fn compute_unknown_tagged_fields_size(
    unknown_tagged_fields: &BTreeMap<i32, Bytes>,
) -> Result<usize> {
    let mut total_size = 0;
    for (&k, v) in unknown_tagged_fields {
        if v.len() > u32::MAX as usize {
            bail!("Tagged field is too long to encode ({} bytes)", v.len());
        }
        total_size += types::UnsignedVarInt.compute_size(k as u32)?;
        total_size += types::UnsignedVarInt.compute_size(v.len() as u32)?;
        total_size += v.len();
    }
    Ok(total_size)
}
