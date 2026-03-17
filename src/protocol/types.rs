//! Raw types of the Kafka protocol, as defined by the protocol specification.
//!
//! In general, most types map closely to a corresponding rust type, with the exception of a number
//! types that use zigzag encoded to represent length as a "compact" representation.
//!
//! It is unnecessary to interact directly with these types for most use cases.
use super::{Decodable, Decoder, Encodable, Encoder, NewType, StrBytes};
use crate::error::{bail, Result};
use crate::protocol::buf::{ByteBuf, ByteBufMut};
use std::convert::TryFrom;
use std::string::String as StdString;

macro_rules! define_copy_impl {
    ($e:ident, $t:ty) => (
        impl Encoder<$t> for $e {
            fn encode<B: ByteBufMut>(&self, buf: &mut B, value: $t) -> Result<()> {
                self.encode(buf, &value)
            }
            fn compute_size(&self, value: $t) -> Result<usize> {
                self.compute_size(&value)
            }
            fn fixed_size(&self) -> Option<usize> {
                <Self as Encoder<&$t>>::fixed_size(self)
            }
        }
    )
}

/// A boolean value.
#[derive(Debug, Copy, Clone, Default)]
pub struct Boolean;

impl<T: NewType<bool>> Encoder<&T> for Boolean {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        buf.put_u8(if *value.borrow() { 1 } else { 0 });
        Ok(())
    }
    fn compute_size(&self, _value: &T) -> Result<usize> {
        Ok(1)
    }
    fn fixed_size(&self) -> Option<usize> {
        Some(1)
    }
}

impl<T: NewType<bool>> Decoder<T> for Boolean {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        Ok((buf.try_get_u8()? != 0).into())
    }
}

define_copy_impl!(Boolean, bool);

macro_rules! define_simple_ints {
    ($($name:ident: $t:ty [$put:ident, $get:ident],)*) => (
        $(
            /// A struct representing [`$ty`]
            #[derive(Debug, Copy, Clone)]
            pub struct $name;

            impl<T: NewType<$t>> Encoder<&T> for $name {
                fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
                    Ok(buf.$put(*value.borrow()))
                }
                fn compute_size(&self, _value: &T) -> Result<usize> {
                    Ok(std::mem::size_of::<$t>())
                }
                fn fixed_size(&self) -> Option<usize> {
                    Some(std::mem::size_of::<$t>())
                }
            }

            impl<T: NewType<$t>> Decoder<T> for $name {
                fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
                    Ok(buf.$get()?.into())
                }
            }

            define_copy_impl!($name, $t);
        )*
    )
}

define_simple_ints! {
    Int8: i8 [put_i8, try_get_i8],
    Int16: i16 [put_i16, try_get_i16],
    UInt16: u16 [put_u16, try_get_u16],
    Int32: i32 [put_i32, try_get_i32],
    Int64: i64 [put_i64, try_get_i64],
    UInt32: u32 [put_u32, try_get_u32],
    Float64: f64 [put_f64, try_get_f64],
}

/// An unsigned zigzag encoded int.
#[derive(Debug, Copy, Clone, Default)]
pub struct UnsignedVarInt;

impl<T: NewType<u32>> Encoder<&T> for UnsignedVarInt {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        let mut value = *value.borrow();
        while value >= 0x80 {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
        Ok(())
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        let value = *value.borrow();
        Ok(match value {
            0x0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1fffff => 3,
            0x200000..=0xfffffff => 4,
            0x10000000..=0xffffffff => 5,
        })
    }
}

impl<T: NewType<u32>> Decoder<T> for UnsignedVarInt {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        let mut value = 0;
        for i in 0..5 {
            let b = buf.try_get_u8()? as u32;
            value |= (b & 0x7F) << (i * 7);
            if b < 0x80 {
                break;
            }
        }
        Ok(value.into())
    }
}

define_copy_impl!(UnsignedVarInt, u32);

/// An unsigned zigzag encoded long.
#[derive(Debug, Copy, Clone, Default)]
pub struct UnsignedVarLong;

impl<T: NewType<u64>> Encoder<&T> for UnsignedVarLong {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        let mut value = *value.borrow();
        while value >= 0x80 {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
        Ok(())
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        let value = *value.borrow();
        Ok(match value {
            0x0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1fffff => 3,
            0x200000..=0xfffffff => 4,
            0x10000000..=0x7ffffffff => 5,
            0x800000000..=0x3ffffffffff => 6,
            0x40000000000..=0x1ffffffffffff => 7,
            0x2000000000000..=0xffffffffffffff => 8,
            0x100000000000000..=0x7fffffffffffffff => 9,
            0x8000000000000000..=0xffffffffffffffff => 10,
        })
    }
}

impl<T: NewType<u64>> Decoder<T> for UnsignedVarLong {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        let mut value = 0;
        for i in 0..10 {
            let b = buf.try_get_u8()? as u64;
            value |= (b & 0x7F) << (i * 7);
            if b < 0x80 {
                break;
            }
        }
        Ok(value.into())
    }
}

define_copy_impl!(UnsignedVarLong, u64);

/// A zizag encoded int.
#[derive(Debug, Copy, Clone, Default)]
pub struct VarInt;

impl<T: NewType<i32>> Encoder<&T> for VarInt {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        let value = *value.borrow();
        let zigzag = ((value << 1) ^ (value >> 31)) as u32;
        UnsignedVarInt.encode(buf, zigzag)
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        let value = *value.borrow();
        let zigzag = ((value << 1) ^ (value >> 31)) as u32;
        UnsignedVarInt.compute_size(zigzag)
    }
}

impl<T: NewType<i32>> Decoder<T> for VarInt {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        let zigzag: u32 = UnsignedVarInt.decode(buf)?;
        Ok((((zigzag >> 1) as i32) ^ (-((zigzag & 1) as i32))).into())
    }
}

define_copy_impl!(VarInt, i32);

/// A zizag encoded long.
#[derive(Debug, Copy, Clone, Default)]
pub struct VarLong;

impl<T: NewType<i64>> Encoder<&T> for VarLong {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        let value = *value.borrow();
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        UnsignedVarLong.encode(buf, &zigzag)
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        let value = *value.borrow();
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        UnsignedVarLong.compute_size(zigzag)
    }
}

impl<T: NewType<i64>> Decoder<T> for VarLong {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        let zigzag: u64 = UnsignedVarLong.decode(buf)?;
        Ok((((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64))).into())
    }
}

define_copy_impl!(VarLong, i64);

/// A v4 UUID.
#[derive(Debug, Copy, Clone, Default)]
pub struct Uuid;

impl<T: NewType<uuid::Uuid>> Encoder<&T> for Uuid {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        buf.put_slice(value.borrow().as_bytes());
        Ok(())
    }
    fn compute_size(&self, _value: &T) -> Result<usize> {
        Ok(16)
    }
    fn fixed_size(&self) -> Option<usize> {
        Some(16)
    }
}

impl<T: NewType<uuid::Uuid>> Decoder<T> for Uuid {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        let mut result = [0; 16];
        buf.try_copy_to_slice(&mut result)?;
        Ok(uuid::Uuid::from_bytes(result).into())
    }
}

define_copy_impl!(Uuid, uuid::Uuid);

/// A string of length up to [`i16::MAX`].
#[derive(Debug, Copy, Clone, Default)]
pub struct String;

impl Encoder<Option<&str>> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&str>) -> Result<()> {
        if let Some(s) = value {
            if s.len() > i16::MAX as usize {
                bail!("String is too long to encode ({} bytes)", s.len());
            } else {
                Int16.encode(buf, s.len() as i16)?;
                buf.put_slice(s.as_bytes());
                Ok(())
            }
        } else {
            Int16.encode(buf, -1)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&str>) -> Result<usize> {
        if let Some(s) = value {
            if s.len() > i16::MAX as usize {
                bail!("String is too long to encode ({} bytes)", s.len());
            } else {
                Ok(2 + s.len())
            }
        } else {
            Ok(2)
        }
    }
}

impl Encoder<Option<&StdString>> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&StdString>) -> Result<()> {
        String.encode(buf, value.map(|s| s.as_str()))
    }
    fn compute_size(&self, value: Option<&StdString>) -> Result<usize> {
        String.compute_size(value.map(|s| s.as_str()))
    }
}

impl Encoder<&Option<StdString>> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<StdString>) -> Result<()> {
        String.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<StdString>) -> Result<usize> {
        String.compute_size(value.as_ref())
    }
}

impl<T: NewType<StrBytes>> Encoder<Option<&T>> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&T>) -> Result<()> {
        String.encode(buf, value.map(|s| &**s.borrow()))
    }
    fn compute_size(&self, value: Option<&T>) -> Result<usize> {
        String.compute_size(value.map(|s| &**s.borrow()))
    }
}

impl<T: NewType<StrBytes>> Encoder<&Option<T>> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<T>) -> Result<()> {
        String.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<T>) -> Result<usize> {
        String.compute_size(value.as_ref())
    }
}

impl Encoder<&str> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &str) -> Result<()> {
        String.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &str) -> Result<usize> {
        String.compute_size(Some(value))
    }
}

impl Encoder<&StdString> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &StdString) -> Result<()> {
        String.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &StdString) -> Result<usize> {
        String.compute_size(Some(value))
    }
}

impl<T: NewType<StrBytes>> Encoder<&T> for String {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        String.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        String.compute_size(Some(value))
    }
}

impl Decoder<Option<StdString>> for String {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<StdString>> {
        match Int16.decode(buf)? {
            -1 => Ok(None),
            n if n >= 0 => {
                let mut strbuf = vec![0; n as usize];
                buf.try_copy_to_slice(&mut strbuf)?;
                Ok(Some(std::string::String::from_utf8(strbuf)?))
            }
            n => {
                bail!("String length is negative ({n})");
            }
        }
    }
}

impl<T: NewType<StrBytes>> Decoder<Option<T>> for String {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<T>> {
        match Int16.decode(buf)? {
            -1 => Ok(None),
            n if n >= 0 => {
                let strbuf = StrBytes::try_from(buf.try_get_bytes(n as usize)?)?;
                Ok(Some(strbuf.into()))
            }
            n => {
                bail!("String length is negative ({n})");
            }
        }
    }
}

impl Decoder<StdString> for String {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<StdString> {
        match String.decode(buf) {
            Ok(None) => {
                bail!("String length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

impl<T: NewType<StrBytes>> Decoder<T> for String {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        match String.decode(buf) {
            Ok(None) => {
                bail!("String length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

/// A string whose length is encoded with a `u32` or varint.
#[derive(Debug, Copy, Clone, Default)]
pub struct CompactString;

impl Encoder<Option<&str>> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&str>) -> Result<()> {
        if let Some(s) = value {
            // Use >= because we're going to add one to the length
            if s.len() >= u32::MAX as usize {
                bail!("CompactString is too long to encode ({} bytes)", s.len());
            } else {
                UnsignedVarInt.encode(buf, (s.len() as u32) + 1)?;
                buf.put_slice(s.as_bytes());
                Ok(())
            }
        } else {
            UnsignedVarInt.encode(buf, 0)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&str>) -> Result<usize> {
        if let Some(s) = value {
            // Use >= because we're going to add one to the length
            if s.len() >= u32::MAX as usize {
                bail!("CompactString is too long to encode ({} bytes)", s.len());
            } else {
                Ok(UnsignedVarInt.compute_size((s.len() as u32) + 1)? + s.len())
            }
        } else {
            Ok(1)
        }
    }
}

impl Encoder<Option<&StdString>> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&StdString>) -> Result<()> {
        CompactString.encode(buf, value.map(|s| s.as_str()))
    }
    fn compute_size(&self, value: Option<&StdString>) -> Result<usize> {
        CompactString.compute_size(value.map(|s| s.as_str()))
    }
}

impl<T: NewType<StrBytes>> Encoder<Option<&T>> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&T>) -> Result<()> {
        CompactString.encode(buf, value.map(|s| &**s.borrow()))
    }
    fn compute_size(&self, value: Option<&T>) -> Result<usize> {
        CompactString.compute_size(value.map(|s| &**s.borrow()))
    }
}

impl Encoder<&Option<StdString>> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<StdString>) -> Result<()> {
        CompactString.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<StdString>) -> Result<usize> {
        CompactString.compute_size(value.as_ref())
    }
}

impl<T: NewType<StrBytes>> Encoder<&Option<T>> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<T>) -> Result<()> {
        CompactString.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<T>) -> Result<usize> {
        CompactString.compute_size(value.as_ref())
    }
}

impl Encoder<&str> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &str) -> Result<()> {
        CompactString.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &str) -> Result<usize> {
        CompactString.compute_size(Some(value))
    }
}

impl Encoder<&StdString> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &StdString) -> Result<()> {
        CompactString.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &StdString) -> Result<usize> {
        CompactString.compute_size(Some(value))
    }
}

impl<T: NewType<StrBytes>> Encoder<&T> for CompactString {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        CompactString.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        CompactString.compute_size(Some(value))
    }
}

impl Decoder<Option<StdString>> for CompactString {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<StdString>> {
        match UnsignedVarInt.decode(buf)? {
            0 => Ok(None),
            n => {
                let mut strbuf = vec![0; (n - 1) as usize];
                buf.try_copy_to_slice(&mut strbuf)?;
                Ok(Some(std::string::String::from_utf8(strbuf)?))
            }
        }
    }
}

impl<T: NewType<StrBytes>> Decoder<Option<T>> for CompactString {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<T>> {
        match UnsignedVarInt.decode(buf)? {
            0 => Ok(None),
            n => {
                let strbuf = StrBytes::try_from(buf.try_get_bytes((n - 1) as usize)?)?;
                Ok(Some(strbuf.into()))
            }
        }
    }
}

impl Decoder<StdString> for CompactString {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<StdString> {
        match CompactString.decode(buf) {
            Ok(None) => {
                bail!("CompactString length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

impl<T: NewType<StrBytes>> Decoder<T> for CompactString {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        match CompactString.decode(buf) {
            Ok(None) => {
                bail!("CompactString length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

/// A sequence of bytes, up to [`i32::MAX`] long.
#[derive(Debug, Copy, Clone, Default)]
pub struct Bytes;

impl Encoder<Option<&[u8]>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&[u8]>) -> Result<()> {
        if let Some(s) = value {
            if s.len() > i32::MAX as usize {
                bail!("Data is too long to encode ({} bytes)", s.len());
            } else {
                Int32.encode(buf, s.len() as i32)?;
                buf.put_slice(s);
                Ok(())
            }
        } else {
            Int32.encode(buf, -1)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&[u8]>) -> Result<usize> {
        if let Some(s) = value {
            if s.len() > i32::MAX as usize {
                bail!("Data is too long to encode ({} bytes)", s.len());
            } else {
                Ok(4 + s.len())
            }
        } else {
            Ok(4)
        }
    }
}

impl Encoder<Option<&Vec<u8>>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&Vec<u8>>) -> Result<()> {
        Bytes.encode(buf, value.map(|s| s.as_slice()))
    }
    fn compute_size(&self, value: Option<&Vec<u8>>) -> Result<usize> {
        Bytes.compute_size(value.map(|s| s.as_slice()))
    }
}

impl Encoder<&Option<Vec<u8>>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<Vec<u8>>) -> Result<()> {
        Bytes.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<Vec<u8>>) -> Result<usize> {
        Bytes.compute_size(value.as_ref())
    }
}

impl Encoder<Option<&bytes::Bytes>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&bytes::Bytes>) -> Result<()> {
        if let Some(s) = value {
            if s.len() > i32::MAX as usize {
                bail!("Data is too long to encode ({} bytes)", s.len());
            }
            Int32.encode(buf, s.len() as i32)?;
            buf.put_shared_bytes(s.clone());
            Ok(())
        } else {
            Int32.encode(buf, -1)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&bytes::Bytes>) -> Result<usize> {
        Bytes.compute_size(value.map(|s| &**s))
    }
}

impl Encoder<&Option<bytes::Bytes>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<bytes::Bytes>) -> Result<()> {
        Bytes.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<bytes::Bytes>) -> Result<usize> {
        Bytes.compute_size(value.as_ref())
    }
}

impl Encoder<&[u8]> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        Bytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &[u8]) -> Result<usize> {
        Bytes.compute_size(Some(value))
    }
}

impl Encoder<&Vec<u8>> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Vec<u8>) -> Result<()> {
        Bytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &Vec<u8>) -> Result<usize> {
        Bytes.compute_size(Some(value))
    }
}

impl Encoder<&bytes::Bytes> for Bytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &bytes::Bytes) -> Result<()> {
        Bytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &bytes::Bytes) -> Result<usize> {
        Bytes.compute_size(Some(value))
    }
}

impl Decoder<Option<Vec<u8>>> for Bytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<Vec<u8>>> {
        match Int32.decode(buf)? {
            -1 => Ok(None),
            n if n >= 0 => {
                let mut data = vec![0; n as usize];
                buf.try_copy_to_slice(&mut data)?;
                Ok(Some(data))
            }
            n => {
                bail!("Data length is negative ({n})");
            }
        }
    }
}

impl Decoder<Option<bytes::Bytes>> for Bytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<bytes::Bytes>> {
        match Int32.decode(buf)? {
            -1 => Ok(None),
            n if n >= 0 => Ok(Some(buf.try_get_bytes(n as usize)?)),
            n => {
                bail!("Data length is negative ({n})");
            }
        }
    }
}

impl Decoder<Vec<u8>> for Bytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Vec<u8>> {
        match Bytes.decode(buf) {
            Ok(None) => {
                bail!("Data length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

impl Decoder<bytes::Bytes> for Bytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<bytes::Bytes> {
        match Bytes.decode(buf) {
            Ok(None) => {
                bail!("Data length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

/// A sequence of bytes that is encoded with a `u32` or a varint, depending on size.
#[derive(Debug, Copy, Clone, Default)]
pub struct CompactBytes;

impl Encoder<Option<&[u8]>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&[u8]>) -> Result<()> {
        if let Some(s) = value {
            // Use >= because we're going to add one to the length
            if s.len() >= u32::MAX as usize {
                bail!("CompactBytes is too long to encode ({} bytes)", s.len());
            } else {
                UnsignedVarInt.encode(buf, (s.len() as u32) + 1)?;
                buf.put_slice(s);
                Ok(())
            }
        } else {
            UnsignedVarInt.encode(buf, 0)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&[u8]>) -> Result<usize> {
        if let Some(s) = value {
            // Use >= because we're going to add one to the length
            if s.len() >= u32::MAX as usize {
                bail!("CompactBytes is too long to encode ({} bytes)", s.len());
            } else {
                Ok(UnsignedVarInt.compute_size((s.len() as u32) + 1)? + s.len())
            }
        } else {
            Ok(1)
        }
    }
}

impl Encoder<Option<&Vec<u8>>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&Vec<u8>>) -> Result<()> {
        CompactBytes.encode(buf, value.map(|s| s.as_slice()))
    }
    fn compute_size(&self, value: Option<&Vec<u8>>) -> Result<usize> {
        CompactBytes.compute_size(value.map(|s| s.as_slice()))
    }
}

impl Encoder<&Option<Vec<u8>>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<Vec<u8>>) -> Result<()> {
        CompactBytes.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<Vec<u8>>) -> Result<usize> {
        CompactBytes.compute_size(value.as_ref())
    }
}

impl Encoder<Option<&bytes::Bytes>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&bytes::Bytes>) -> Result<()> {
        if let Some(s) = value {
            if s.len() >= u32::MAX as usize {
                bail!("CompactBytes is too long to encode ({} bytes)", s.len());
            }
            UnsignedVarInt.encode(buf, (s.len() as u32) + 1)?;
            buf.put_shared_bytes(s.clone());
            Ok(())
        } else {
            UnsignedVarInt.encode(buf, 0)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&bytes::Bytes>) -> Result<usize> {
        CompactBytes.compute_size(value.map(|s| &**s))
    }
}

impl Encoder<&Option<bytes::Bytes>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<bytes::Bytes>) -> Result<()> {
        CompactBytes.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<bytes::Bytes>) -> Result<usize> {
        CompactBytes.compute_size(value.as_ref())
    }
}

impl Encoder<&[u8]> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        CompactBytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &[u8]) -> Result<usize> {
        CompactBytes.compute_size(Some(value))
    }
}

impl Encoder<&Vec<u8>> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Vec<u8>) -> Result<()> {
        CompactBytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &Vec<u8>) -> Result<usize> {
        CompactBytes.compute_size(Some(value))
    }
}

impl Encoder<&bytes::Bytes> for CompactBytes {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &bytes::Bytes) -> Result<()> {
        CompactBytes.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &bytes::Bytes) -> Result<usize> {
        CompactBytes.compute_size(Some(value))
    }
}

impl Decoder<Option<Vec<u8>>> for CompactBytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<Vec<u8>>> {
        match UnsignedVarInt.decode(buf)? {
            0 => Ok(None),
            n => {
                let mut data = vec![0; (n - 1) as usize];
                buf.try_copy_to_slice(&mut data)?;
                Ok(Some(data))
            }
        }
    }
}

impl Decoder<Option<bytes::Bytes>> for CompactBytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<bytes::Bytes>> {
        match UnsignedVarInt.decode(buf)? {
            0 => Ok(None),
            n => Ok(Some(buf.try_get_bytes((n - 1) as usize)?)),
        }
    }
}

impl Decoder<Vec<u8>> for CompactBytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Vec<u8>> {
        match CompactBytes.decode(buf) {
            Ok(None) => {
                bail!("Data length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

impl Decoder<bytes::Bytes> for CompactBytes {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<bytes::Bytes> {
        match CompactBytes.decode(buf) {
            Ok(None) => {
                bail!("Data length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

/// A struct, which is encoded according to the type it represents.
#[derive(Debug, Copy, Clone, Default)]
pub struct Struct {
    /// The version of the struct.
    pub version: i16,
}

impl<T: Encodable> Encoder<&T> for Struct {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &T) -> Result<()> {
        value.encode(buf, self.version)
    }
    fn compute_size(&self, value: &T) -> Result<usize> {
        value.compute_size(self.version)
    }
}

impl<T: Decodable> Decoder<T> for Struct {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<T> {
        T::decode(buf, self.version)
    }
}

/// An optional structure
#[derive(Debug, Copy, Clone)]
pub struct OptionStruct {
    /// The version of the struct.
    pub version: i16,
}

impl<T: Encodable> Encoder<&Option<T>> for OptionStruct {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<T>) -> Result<()> {
        if let Some(value) = value {
            Int8.encode(buf, 1)?;
            value.encode(buf, self.version)?;
        } else {
            Int8.encode(buf, -1)?;
        }
        Ok(())
    }

    fn compute_size(&self, value: &Option<T>) -> Result<usize> {
        Ok(match value {
            Some(value) => 1 + value.compute_size(self.version)?,
            None => 1,
        })
    }
}

impl<T: Decodable> Decoder<Option<T>> for OptionStruct {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<T>> {
        let present: i8 = Int8.decode(buf)?;
        if present == 1 {
            Ok(Some(T::decode(buf, self.version)?))
        } else {
            Ok(None)
        }
    }
}

/// An array whose length is encoded with an `i32`.
#[derive(Debug, Copy, Clone)]
pub struct Array<E>(pub E);

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&[T]>> for Array<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&[T]>) -> Result<()> {
        if let Some(a) = value {
            if a.len() > i32::MAX as usize {
                bail!("Array is too long to encode ({} items)", a.len());
            } else {
                Int32.encode(buf, a.len() as i32)?;
                for item in a {
                    self.0.encode(buf, item)?;
                }
                Ok(())
            }
        } else {
            Int32.encode(buf, -1)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&[T]>) -> Result<usize> {
        if let Some(a) = value {
            if a.len() > i32::MAX as usize {
                bail!("Array is too long to encode ({} items)", a.len());
            } else if let Some(fixed_size) = self.0.fixed_size() {
                Ok(4 + a.len() * fixed_size)
            } else {
                let mut total_size = 4;
                for item in a {
                    total_size += self.0.compute_size(item)?;
                }
                Ok(total_size)
            }
        } else {
            Ok(4)
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&Vec<T>>> for Array<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&Vec<T>>) -> Result<()> {
        self.encode(buf, value.map(|s| s.as_slice()))
    }
    fn compute_size(&self, value: Option<&Vec<T>>) -> Result<usize> {
        self.compute_size(value.map(|s| s.as_slice()))
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&Option<Vec<T>>> for Array<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<Vec<T>>) -> Result<()> {
        self.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<Vec<T>>) -> Result<usize> {
        self.compute_size(value.as_ref())
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&[T]> for Array<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &[T]) -> Result<()> {
        self.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &[T]) -> Result<usize> {
        self.compute_size(Some(value))
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&Vec<T>> for Array<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Vec<T>) -> Result<()> {
        self.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &Vec<T>) -> Result<usize> {
        self.compute_size(Some(value))
    }
}

impl<T, E: Decoder<T>> Decoder<Option<Vec<T>>> for Array<E> {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<Vec<T>>> {
        match Int32.decode(buf)? {
            -1 => Ok(None),
            n if n >= 0 => {
                let mut result = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    result.push(self.0.decode(buf)?);
                }
                Ok(Some(result))
            }
            n => {
                bail!("Array length is negative ({n})");
            }
        }
    }
}

impl<T, E: Decoder<T>> Decoder<Vec<T>> for Array<E> {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Vec<T>> {
        match self.decode(buf) {
            Ok(None) => {
                bail!("Array length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

/// An array whose length is encoded with a varint.
#[derive(Debug, Copy, Clone)]
pub struct CompactArray<E>(pub E);

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&[T]>> for CompactArray<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&[T]>) -> Result<()> {
        if let Some(a) = value {
            // Use >= because we're going to add one to the length
            if a.len() >= u32::MAX as usize {
                bail!("CompactArray is too long to encode ({} items)", a.len());
            } else {
                UnsignedVarInt.encode(buf, (a.len() as u32) + 1)?;
                for item in a {
                    self.0.encode(buf, item)?;
                }
                Ok(())
            }
        } else {
            UnsignedVarInt.encode(buf, 0)?;
            Ok(())
        }
    }
    fn compute_size(&self, value: Option<&[T]>) -> Result<usize> {
        if let Some(a) = value {
            // Use >= because we're going to add one to the length
            if a.len() >= u32::MAX as usize {
                bail!("CompactArray is too long to encode ({} items)", a.len());
            } else if let Some(fixed_size) = self.0.fixed_size() {
                Ok(UnsignedVarInt.compute_size((a.len() as u32) + 1)? + a.len() * fixed_size)
            } else {
                let mut total_size = UnsignedVarInt.compute_size((a.len() as u32) + 1)?;
                for item in a {
                    total_size += self.0.compute_size(item)?;
                }
                Ok(total_size)
            }
        } else {
            Ok(1)
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&Vec<T>>> for CompactArray<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: Option<&Vec<T>>) -> Result<()> {
        self.encode(buf, value.map(|s| s.as_slice()))
    }
    fn compute_size(&self, value: Option<&Vec<T>>) -> Result<usize> {
        self.compute_size(value.map(|s| s.as_slice()))
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&Option<Vec<T>>> for CompactArray<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Option<Vec<T>>) -> Result<()> {
        self.encode(buf, value.as_ref())
    }
    fn compute_size(&self, value: &Option<Vec<T>>) -> Result<usize> {
        self.compute_size(value.as_ref())
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&[T]> for CompactArray<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &[T]) -> Result<()> {
        self.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &[T]) -> Result<usize> {
        self.compute_size(Some(value))
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&Vec<T>> for CompactArray<E> {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, value: &Vec<T>) -> Result<()> {
        self.encode(buf, Some(value))
    }
    fn compute_size(&self, value: &Vec<T>) -> Result<usize> {
        self.compute_size(Some(value))
    }
}

impl<T, E: Decoder<T>> Decoder<Option<Vec<T>>> for CompactArray<E> {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Option<Vec<T>>> {
        match UnsignedVarInt.decode(buf)? {
            0 => Ok(None),
            n => {
                let mut result = Vec::with_capacity((n - 1) as usize);
                for _ in 1..n {
                    result.push(self.0.decode(buf)?);
                }
                Ok(Some(result))
            }
        }
    }
}

impl<T, E: Decoder<T>> Decoder<Vec<T>> for CompactArray<E> {
    fn decode<B: ByteBuf>(&self, buf: &mut B) -> Result<Vec<T>> {
        match self.decode(buf) {
            Ok(None) => {
                bail!("CompactArray length is negative (-1)");
            }
            Ok(Some(s)) => Ok(s),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt::Debug;

    fn test_encoder_decoder<V: PartialEq + Debug, E: for<'a> Encoder<&'a V> + Decoder<V>>(
        encoder: E,
        value: V,
        mut expected: &[u8],
    ) {
        let mut buf = vec![];
        encoder.encode(&mut buf, &value).unwrap();
        assert_eq!(buf, expected);

        let decoded = encoder.decode(&mut expected).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn smoke_varint_encoder_decoder() {
        test_encoder_decoder(VarInt, 0, &[0]);
        test_encoder_decoder(VarInt, -1, &[1]);
        test_encoder_decoder(VarInt, 1, &[2]);
        test_encoder_decoder(VarInt, -2, &[3]);
        test_encoder_decoder(VarInt, 300, &[216, 4]);
        test_encoder_decoder(VarInt, i32::MAX, &[254, 255, 255, 255, 15]);
        test_encoder_decoder(VarInt, i32::MIN, &[255, 255, 255, 255, 15]);
    }

    #[test]
    fn smoke_varlong_encoder_decoder() {
        test_encoder_decoder(VarLong, 0, &[0]);
        test_encoder_decoder(VarLong, -1, &[1]);
        test_encoder_decoder(VarLong, 1, &[2]);
        test_encoder_decoder(VarLong, -2, &[3]);
        test_encoder_decoder(VarLong, 300, &[216, 4]);
        test_encoder_decoder(
            VarLong,
            i64::MAX,
            &[254, 255, 255, 255, 255, 255, 255, 255, 255, 1],
        );
        test_encoder_decoder(
            VarLong,
            i64::MIN,
            &[255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
        );
    }

    #[test]
    fn smoke_string_encoder_decoder() {
        test_encoder_decoder(
            String,
            std::string::String::from("hello"),
            &[0, 5, 104, 101, 108, 108, 111],
        );
        test_encoder_decoder(String, None::<std::string::String>, &[255, 255]);
    }

    #[test]
    fn smoke_compact_string_encoder_decoder() {
        test_encoder_decoder(
            CompactString,
            std::string::String::from("hello"),
            &[6, 104, 101, 108, 108, 111],
        );
        test_encoder_decoder(CompactString, None::<std::string::String>, &[0]);
    }

    #[test]
    fn smoke_bytes_encoder_decoder() {
        test_encoder_decoder(Bytes, vec![1, 2, 3, 4], &[0, 0, 0, 4, 1, 2, 3, 4]);
        test_encoder_decoder(Bytes, None::<Vec<u8>>, &[255, 255, 255, 255]);
    }

    #[test]
    fn smoke_compact_bytes_encoder_decoder() {
        test_encoder_decoder(CompactBytes, vec![1, 2, 3, 4], &[5, 1, 2, 3, 4]);
        test_encoder_decoder(CompactBytes, None::<Vec<u8>>, &[0]);
    }
}
