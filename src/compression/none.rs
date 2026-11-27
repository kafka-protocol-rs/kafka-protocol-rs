use crate::protocol::buf::{ByteBuf, ByteBufMut};
use anyhow::Result;

use super::{BoundedDecompressor, Compressor, Decompressor};
use crate::records::DecodeBudget;

/// Noop compression implementation.
pub struct None;

impl<B: ByteBufMut> Compressor<B> for None {
    type BufMut = B;
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        f(buf)
    }
}

impl<B: ByteBuf> Decompressor<B> for None {
    type Buf = B;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_budget(buf, &mut DecodeBudget::default(), |decoded, _| f(decoded))
    }
}

impl<B: ByteBuf> BoundedDecompressor<B> for None {
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>,
    {
        budget.charge(
            crate::records::DecodeResource::DecodedBytes,
            buf.remaining(),
        )?;
        f(buf, budget)
    }
}
