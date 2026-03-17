//! Provides compression utilities for encoding records.
//!
//! This module has implementations of gzip, Snappy, as well as a noop compression format that
//! allows encoding and decoding records into a [`Record`](crate::records::Record).

use crate::error::Result;
use crate::protocol::buf::{ByteBuf, ByteBufMut};

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "lz4")]
mod lz4;
mod none;
#[cfg(feature = "snappy")]
mod snappy;
#[cfg(feature = "zstd")]
mod zstd;

#[cfg(feature = "gzip")]
pub use gzip::Gzip;
#[cfg(feature = "lz4")]
pub use lz4::Lz4;
pub use none::None;
#[cfg(feature = "snappy")]
pub use snappy::Snappy;
#[cfg(feature = "zstd")]
pub use zstd::Zstd;

/// A trait for record compression algorithms.
pub trait Compressor<B: ByteBufMut> {
    /// Target buffer type for compression.
    type BufMut: ByteBufMut;
    /// Compresses into provided [`ByteBufMut`], with records encoded by `F` into `R`.
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>;
}

/// A trait for record decompression algorithms.
pub trait Decompressor<B: ByteBuf> {
    /// Target buffer type for decompression.
    type Buf: ByteBuf;
    /// Decompress records from `B` mapped using `F` into `R`.
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>;
}
