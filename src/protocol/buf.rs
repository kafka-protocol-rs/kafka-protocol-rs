//! Utilities for working with the [`bytes`] crate.
use std::io::Cursor;
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Error indicating there are not enough remaining bytes in a buffer to perform a read.
#[derive(Debug)]
pub struct NotEnoughBytesError;

impl Display for NotEnoughBytesError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Not enough bytes remaining in buffer!")
    }
}

impl Error for NotEnoughBytesError {}

/// Extension for working with [`bytes::Buf`].
pub trait ByteBuf: Buf {
    /// Peek ahead in the buffer by the provided range.
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes;
    /// Get `size` bytes from the underlying buffer.
    fn get_bytes(&mut self, size: usize) -> Bytes;
    /// Try to peek ahead in the buffer by the provided range, returning an error if there are less
    /// bytes than the requested range.
    fn try_peek_bytes(&mut self, r: Range<usize>) -> Result<Bytes, NotEnoughBytesError> {
        if self.remaining() < r.end {
            Err(NotEnoughBytesError)
        } else {
            Ok(self.peek_bytes(r))
        }
    }
    /// Try to get `size` bytes from the buffer, returning an error if there are less bytes than the
    /// requested number.
    fn try_get_bytes(&mut self, size: usize) -> Result<Bytes, NotEnoughBytesError> {
        if self.remaining() < size {
            Err(NotEnoughBytesError)
        } else {
            Ok(self.get_bytes(size))
        }
    }
}

impl ByteBuf for Bytes {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        self.slice(r)
    }
    fn get_bytes(&mut self, size: usize) -> Bytes {
        self.split_to(size)
    }
}

impl ByteBuf for BytesMut {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        Bytes::copy_from_slice(&self[r])
    }
    fn get_bytes(&mut self, size: usize) -> Bytes {
        self.split_to(size).freeze()
    }
}

impl<T: ByteBuf> ByteBuf for &mut T {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        (**self).peek_bytes(r)
    }
    fn get_bytes(&mut self, size: usize) -> Bytes {
        (**self).get_bytes(size)
    }
    fn try_peek_bytes(&mut self, r: Range<usize>) -> Result<Bytes, NotEnoughBytesError> {
        (**self).try_peek_bytes(r)
    }
    fn try_get_bytes(&mut self, size: usize) -> Result<Bytes, NotEnoughBytesError> {
        (**self).try_get_bytes(size)
    }
}

impl ByteBuf for &[u8] {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        Bytes::copy_from_slice(&self[r])
    }
    fn get_bytes(&mut self, size: usize) -> Bytes {
        let (a, b) = self.split_at(size);
        *self = b;
        Bytes::copy_from_slice(a)
    }
}

impl<T: AsRef<[u8]>> ByteBuf for Cursor<T> {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        Bytes::copy_from_slice(&self.get_ref().as_ref()[r])
    }
    fn get_bytes(&mut self, size: usize) -> Bytes {
        let pos = self.position() as usize;
        self.set_position((pos + size) as u64);
        Bytes::copy_from_slice(&self.get_ref().as_ref()[pos..(pos + size)])
    }
}

/// A gap of specified length at the specified offset.
#[derive(Debug, Copy, Clone)]
pub struct Gap {
    offset: usize,
    len: usize,
}

/// A type capable of being represented as a gap in a buffer.
pub trait GapType {
    /// The type of the gap.
    type Value;
    /// The size of the gap.
    fn size(&self) -> usize;
    /// Insert a value into the provided buffer.
    fn put(&self, buf: &mut [u8], value: Self::Value);
}

/// A gap of type `T`.
pub struct TypedGap<T> {
    gap: Gap,
    type_: T,
}

macro_rules! define_gap_types {
    {$($n:ident => $f:ident($t:ty)),*$(,)*} => {
        /// Types which implement `GapType`.
        pub mod gap {
            use super::*;
            $(
                #[derive(Copy, Clone, Debug)]
                pub(crate) struct $n;

                impl GapType for $n {
                    type Value = $t;
                    fn size(&self) -> usize {
                        std::mem::size_of::<Self::Value>()
                    }
                    fn put(&self, mut buf: &mut [u8], value: Self::Value) {
                        buf.$f(value);
                    }
                }
            )*
        }
    };
}

define_gap_types! {
    U32 => put_u32(u32),
    I32 => put_i32(i32),
}

/// Extension for working with [`bytes::buf::BufMut`].
pub trait ByteBufMut: BufMut {
    /// Get the current offset of the buffer.
    fn offset(&self) -> usize;

    /// Seek to the provided offset in the buffer.
    fn seek(&mut self, offset: usize);

    /// Read a range from the buffer.
    fn range(&mut self, r: Range<usize>) -> &mut [u8];

    /// Put a gap of `len` at the current buffer offset.
    fn put_gap(&mut self, len: usize) -> Gap {
        let res = Gap {
            offset: self.offset(),
            len,
        };
        self.seek(res.offset + len);
        res
    }

    /// Read a gap from the buffer.
    fn gap_buf(&mut self, gap: Gap) -> &mut [u8] {
        self.range(gap.offset..(gap.offset + gap.len))
    }

    /// Put a typed gap of type `T` at the current buffer offset.
    fn put_typed_gap<T: GapType>(&mut self, type_: T) -> TypedGap<T> {
        TypedGap {
            gap: self.put_gap(type_.size()),
            type_,
        }
    }

    /// Insert a value of the [`TypedGap`] type at the current buffer offset.
    fn fill_typed_gap<T: GapType>(&mut self, gap: TypedGap<T>, value: T::Value) {
        gap.type_.put(self.gap_buf(gap.gap), value);
    }
}

impl ByteBufMut for BytesMut {
    fn offset(&self) -> usize {
        self.len()
    }
    fn seek(&mut self, offset: usize) {
        self.resize(offset, 0);
    }
    fn range(&mut self, r: Range<usize>) -> &mut [u8] {
        &mut self[r]
    }
}

impl ByteBufMut for Vec<u8> {
    fn offset(&self) -> usize {
        self.len()
    }
    fn seek(&mut self, offset: usize) {
        self.resize(offset, 0);
    }
    fn range(&mut self, r: Range<usize>) -> &mut [u8] {
        &mut self[r]
    }
}

impl<T: ByteBufMut> ByteBufMut for &mut T {
    fn offset(&self) -> usize {
        (**self).offset()
    }
    fn seek(&mut self, offset: usize) {
        (**self).seek(offset)
    }
    fn range(&mut self, r: Range<usize>) -> &mut [u8] {
        (**self).range(r)
    }
}
