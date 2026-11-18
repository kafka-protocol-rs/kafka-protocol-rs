use std::io::Write;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::{GzDecoder, GzEncoder};
use flate2::Compression;

use crate::protocol::buf::{ByteBuf, ByteBufMut};

use super::bounded::BoundedWriter;
use super::{BoundedDecompressor, Compressor, Decompressor};
use crate::records::DecodeBudget;

/// Gzip compression algorithm. See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
/// for more information.
pub struct Gzip;

impl<B: ByteBufMut> Compressor<B> for Gzip {
    type BufMut = BytesMut;
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        // Write uncompressed bytes into a temporary buffer
        let mut tmp = BytesMut::new();
        let res = f(&mut tmp)?;

        // Compress directly into the target buffer
        let mut e = GzEncoder::new(buf.writer(), Compression::default());
        e.write_all(&tmp).context("Failed to compress gzip")?;
        e.finish().context("Failed to compress gzip")?;

        Ok(res)
    }
}

impl<B: ByteBuf> Decompressor<B> for Gzip {
    type Buf = Bytes;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_budget(buf, &mut DecodeBudget::default(), |decoded, _| f(decoded))
    }
}

impl<B: ByteBuf> BoundedDecompressor<B> for Gzip {
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>,
    {
        budget.check()?;
        let mut output = BoundedWriter::new(budget)?;
        let result = {
            let mut decoder = GzDecoder::new(&mut output);
            let written = decoder.write_all(&buf.copy_to_bytes(buf.remaining()));
            let finished = decoder.finish().map(|_| ());
            written.and(finished)
        };
        let mut decoded = output.finish(result).context("Failed to decompress gzip")?;
        f(&mut decoded, budget)
    }
}
