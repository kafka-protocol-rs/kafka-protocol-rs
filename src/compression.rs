//! Provides compression utilities for encoding records.
//!
//! This module has implementations of gzip, Snappy, as well as a noop compression format that
//! allows encoding and decoding records into a [`Record`](crate::records::Record).

use crate::protocol::buf::{ByteBuf, ByteBufMut};
use crate::records::DecodeBudget;
use anyhow::Result;

#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
mod bounded;

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
///
/// Built-in implementations use a fresh default [`DecodeBudget`] per call.
/// Use [`BoundedDecompressor`] to select limits or share accounting across calls.
/// Third-party implementations of this trait are responsible for their own limits.
pub trait Decompressor<B: ByteBuf> {
    /// Target buffer type for decompression.
    type Buf: ByteBuf;
    /// Decompress records from `B` mapped using `F` into `R`.
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>;
}

/// Decompression that enforces a shared output budget before allocation/growth.
///
/// There is no fallback to unbounded decompression. Implementations must check the
/// budget before starting, charge decoded bytes before growing output, and enforce
/// codec workspace policy. The callback runs only after successful completion and
/// receives the same budget for further accounting. Callback allocations are the
/// caller's responsibility; built-ins charge bytes, not record/header/batch counts.
pub trait BoundedDecompressor<B: ByteBuf>: Decompressor<B> {
    /// Decompress within `budget`, then map the decoded buffer using `f`.
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>;
}

#[cfg(test)]
mod tests;
