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
    U8 => put_u8(u8),
    I8 => put_i8(i8),
    U16 => put_u16(u16),
    U16Le => put_u16_le(u16),
    I16 => put_i16(i16),
    I16Le => put_i16_le(i16),
    U32 => put_u32(u32),
    U32Le => put_u32_le(u32),
    I32 => put_i32(i32),
    I32Le => put_i32_le(i32),
    U64 => put_u64(u64),
    U64Le => put_u64_le(u64),
    I64 => put_i64(i64),
    I64Le => put_i64_le(i64),
    U128 => put_u128(u128),
    U128Le => put_u128_le(u128),
    I128 => put_i128(i128),
    I128Le => put_i128_le(i128),
    F32 => put_f32(f32),
    F32Le => put_f32_le(f32),
    F64 => put_f64(f64),
    F64Le => put_f64_le(f64),
}

/// Extension for working with [`bytes::buf::BufMut`].
pub trait ByteBufMut: BufMut {
    /// Get the current offset of the buffer.
    fn offset(&self) -> usize;

    /// Seek to the provided offset in the buffer.
    fn seek(&mut self, offset: usize);

    /// Read a range from the buffer.
    fn range(&mut self, r: Range<usize>) -> &mut [u8];

    /// Store a shared `Bytes` handle without copying.
    ///
    /// The default implementation copies the bytes (matching existing behavior).
    /// `SegmentedBuf` overrides this to store a zero-copy reference.
    fn put_shared_bytes(&mut self, bytes: Bytes) {
        self.put_slice(&bytes);
    }

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
    fn put_shared_bytes(&mut self, bytes: Bytes) {
        (**self).put_shared_bytes(bytes)
    }
}

/// A segment in a [`SegmentedBuf`].
enum Segment {
    /// Inline bytes that were written via normal BufMut methods.
    Inline(BytesMut),
    /// A shared `Bytes` handle stored without copying.
    Shared(Bytes),
}

impl Segment {
    fn len(&self) -> usize {
        match self {
            Segment::Inline(b) => b.len(),
            Segment::Shared(b) => b.len(),
        }
    }
}

/// A segmented buffer that stores a mix of inline writes and shared `Bytes` references.
///
/// Normal `BufMut` writes go into an inline `BytesMut` segment. Calling `put_shared_bytes`
/// finalizes the current inline segment and appends a zero-copy `Shared` segment.
///
/// Implements `bytes::Buf` with `chunks_vectored()` so that `tokio::io::AsyncWriteExt::write_all_buf`
/// can use vectored I/O (`writev`) to write all segments in a single syscall.
#[derive(Default)]
pub struct SegmentedBuf {
    segments: Vec<Segment>,
    /// Total byte count across all segments.
    total_len: usize,
    /// Bytes already consumed by `Buf::advance`.
    consumed: usize,
}

impl SegmentedBuf {
    /// Create a new empty segmented buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure the last segment is an `Inline` and return a mutable reference to it.
    fn current_inline(&mut self) -> &mut BytesMut {
        if self.segments.is_empty() || !matches!(self.segments.last(), Some(Segment::Inline(_))) {
            self.segments.push(Segment::Inline(BytesMut::new()));
        }
        match self.segments.last_mut().unwrap() {
            Segment::Inline(b) => b,
            _ => unreachable!(),
        }
    }

    /// Write an `i32` at the given absolute byte offset within the buffer.
    ///
    /// Used to patch the length prefix after the full message has been written.
    pub fn patch_i32(&mut self, offset: usize, value: i32) {
        let bytes = value.to_be_bytes();
        let mut pos = 0;
        for seg in &mut self.segments {
            let seg_len = seg.len();
            if offset >= pos && offset + 4 <= pos + seg_len {
                let local = offset - pos;
                match seg {
                    Segment::Inline(b) => {
                        b[local..local + 4].copy_from_slice(&bytes);
                    }
                    Segment::Shared(_) => {
                        panic!("Cannot patch into a Shared segment");
                    }
                }
                return;
            }
            pos += seg_len;
        }
        panic!(
            "patch_i32: offset {} out of range (total {})",
            offset, self.total_len
        );
    }
}

unsafe impl BufMut for SegmentedBuf {
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.total_len
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        let inline = self.current_inline();
        unsafe { inline.advance_mut(cnt) };
        self.total_len += cnt;
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        let inline = self.current_inline();
        if inline.capacity() == inline.len() {
            inline.reserve(8192);
        }
        inline.chunk_mut()
    }

    fn put_slice(&mut self, src: &[u8]) {
        let inline = self.current_inline();
        inline.put_slice(src);
        self.total_len += src.len();
    }
}

impl ByteBufMut for SegmentedBuf {
    fn offset(&self) -> usize {
        self.total_len
    }

    fn seek(&mut self, offset: usize) {
        if offset > self.total_len {
            let needed = offset - self.total_len;
            let inline = self.current_inline();
            inline.resize(inline.len() + needed, 0);
            self.total_len = offset;
        }
    }

    fn range(&mut self, r: Range<usize>) -> &mut [u8] {
        let mut pos = 0;
        for seg in &mut self.segments {
            let seg_len = seg.len();
            if r.start >= pos && r.end <= pos + seg_len {
                let local_start = r.start - pos;
                let local_end = r.end - pos;
                return match seg {
                    Segment::Inline(b) => &mut b[local_start..local_end],
                    Segment::Shared(_) => panic!("Cannot get mutable range from Shared segment"),
                };
            }
            pos += seg_len;
        }
        panic!("range {:?} out of bounds (total {})", r, self.total_len);
    }

    fn put_shared_bytes(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        let len = bytes.len();
        if let Some(Segment::Inline(b)) = self.segments.last() {
            if b.is_empty() {
                self.segments.pop();
            }
        }
        self.segments.push(Segment::Shared(bytes));
        self.total_len += len;
    }
}

impl Buf for SegmentedBuf {
    fn remaining(&self) -> usize {
        self.total_len - self.consumed
    }

    fn chunk(&self) -> &[u8] {
        let mut pos = 0;
        for seg in &self.segments {
            let seg_len = seg.len();
            if self.consumed < pos + seg_len {
                let local_offset = self.consumed - pos;
                return match seg {
                    Segment::Inline(b) => &b[local_offset..],
                    Segment::Shared(b) => &b[local_offset..],
                };
            }
            pos += seg_len;
        }
        &[]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining(),
            "advance({}) exceeds remaining({})",
            cnt,
            self.remaining()
        );
        self.consumed += cnt;
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        if dst.is_empty() {
            return 0;
        }
        let mut filled = 0;
        let mut pos = 0;
        for seg in &self.segments {
            let seg_len = seg.len();
            if self.consumed >= pos + seg_len {
                pos += seg_len;
                continue;
            }
            let local_offset = self.consumed.saturating_sub(pos);
            let slice = match seg {
                Segment::Inline(b) => &b[local_offset..],
                Segment::Shared(b) => &b[local_offset..],
            };
            if !slice.is_empty() {
                dst[filled] = std::io::IoSlice::new(slice);
                filled += 1;
                if filled >= dst.len() {
                    break;
                }
            }
            pos += seg_len;
        }
        filled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmented_buf_inline_only() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"hello ");
        buf.put_slice(b"world");
        assert_eq!(buf.remaining(), 11);
        assert_eq!(buf.chunk(), b"hello world");
    }

    #[test]
    fn segmented_buf_shared_only() {
        let mut buf = SegmentedBuf::new();
        buf.put_shared_bytes(Bytes::from_static(b"hello "));
        buf.put_shared_bytes(Bytes::from_static(b"world"));
        assert_eq!(buf.remaining(), 11);
        // chunk() returns the first segment
        assert_eq!(buf.chunk(), b"hello ");
    }

    #[test]
    fn segmented_buf_mixed_inline_and_shared() {
        let mut buf = SegmentedBuf::new();
        // Write header inline
        buf.put_slice(b"HDR:");
        // Append shared record data
        buf.put_shared_bytes(Bytes::from_static(b"record1"));
        // Write more inline
        buf.put_slice(b"|");
        buf.put_shared_bytes(Bytes::from_static(b"record2"));

        assert_eq!(buf.remaining(), 4 + 7 + 1 + 7);

        // Read all bytes by advancing through chunks
        let mut collected = Vec::new();
        while buf.has_remaining() {
            let chunk = buf.chunk();
            collected.extend_from_slice(chunk);
            let len = chunk.len();
            buf.advance(len);
        }
        assert_eq!(&collected, b"HDR:record1|record2");
    }

    #[test]
    fn segmented_buf_empty_shared_ignored() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"data");
        buf.put_shared_bytes(Bytes::new()); // empty, should be ignored
        assert_eq!(buf.remaining(), 4);
        assert_eq!(buf.chunk(), b"data");
    }

    #[test]
    fn segmented_buf_advance_across_segments() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"ab");
        buf.put_shared_bytes(Bytes::from_static(b"cd"));
        buf.put_slice(b"ef");

        assert_eq!(buf.remaining(), 6);
        buf.advance(1); // consume 'a'
        assert_eq!(buf.remaining(), 5);
        assert_eq!(buf.chunk(), b"b");
        buf.advance(1); // consume 'b'
        assert_eq!(buf.chunk(), b"cd");
        buf.advance(3); // consume 'cd' and 'e'
        assert_eq!(buf.chunk(), b"f");
        buf.advance(1);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn segmented_buf_chunks_vectored() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"header");
        buf.put_shared_bytes(Bytes::from_static(b"body1"));
        buf.put_shared_bytes(Bytes::from_static(b"body2"));
        buf.put_slice(b"trailer");

        let mut io_slices = [std::io::IoSlice::new(&[]); 8];
        let n = buf.chunks_vectored(&mut io_slices);
        assert_eq!(n, 4);
        assert_eq!(&*io_slices[0], b"header");
        assert_eq!(&*io_slices[1], b"body1");
        assert_eq!(&*io_slices[2], b"body2");
        assert_eq!(&*io_slices[3], b"trailer");
    }

    #[test]
    fn segmented_buf_chunks_vectored_after_partial_advance() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"hdr");
        buf.put_shared_bytes(Bytes::from_static(b"payload"));

        // Advance past the inline segment into the shared one
        buf.advance(4); // consume "hdr" + first byte of "payload"
        let mut io_slices = [std::io::IoSlice::new(&[]); 4];
        let n = buf.chunks_vectored(&mut io_slices);
        assert_eq!(n, 1);
        assert_eq!(&*io_slices[0], b"ayload");
    }

    #[test]
    fn segmented_buf_patch_i32() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(&[0u8; 4]); // placeholder
        buf.put_slice(b"data");
        buf.patch_i32(0, 42);

        assert_eq!(buf.chunk()[..4], 42i32.to_be_bytes());
    }

    #[test]
    #[should_panic(expected = "Cannot patch into a Shared segment")]
    fn segmented_buf_patch_i32_into_shared_panics() {
        let mut buf = SegmentedBuf::new();
        buf.put_shared_bytes(Bytes::from_static(&[0u8; 4]));
        buf.patch_i32(0, 42);
    }

    #[test]
    fn segmented_buf_default() {
        let buf = SegmentedBuf::default();
        assert_eq!(buf.remaining(), 0);
        assert!(buf.chunk().is_empty());
    }

    #[test]
    fn byte_buf_mut_put_shared_bytes_default_copies() {
        // The default `put_shared_bytes` on BytesMut should copy (no-op zero-copy)
        let mut buf = BytesMut::new();
        buf.put_shared_bytes(Bytes::from_static(b"hello"));
        assert_eq!(&buf[..], b"hello");
    }

    #[test]
    fn segmented_buf_bytebufmut_offset_and_seek() {
        let mut buf = SegmentedBuf::new();
        assert_eq!(buf.offset(), 0);
        buf.put_slice(b"abc");
        assert_eq!(buf.offset(), 3);
        buf.seek(10);
        assert_eq!(buf.offset(), 10);
        // seek doesn't shrink
        buf.seek(5);
        assert_eq!(buf.offset(), 10);
    }

    #[test]
    fn segmented_buf_bytebufmut_range() {
        let mut buf = SegmentedBuf::new();
        buf.put_slice(b"hello world");
        let r = buf.range(6..11);
        assert_eq!(r, b"world");
    }
}
