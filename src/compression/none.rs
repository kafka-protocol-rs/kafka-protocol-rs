use crate::error::Result;
use crate::protocol::buf::{ByteBuf, ByteBufMut};

use super::{Compressor, Decompressor};

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
        f(buf)
    }
}
