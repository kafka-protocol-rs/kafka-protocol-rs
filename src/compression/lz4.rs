use crate::protocol::buf::{ByteBuf, ByteBufMut};
use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lz4::BlockMode;
use lz4::{Decoder, EncoderBuilder};
use std::io;

use super::bounded::BoundedWriter;
use super::{BoundedDecompressor, Compressor, Decompressor};
use crate::records::DecodeBudget;

/// Lz4 compression algorithm. See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
/// for more information.
pub struct Lz4;

const COMPRESSION_LEVEL: u32 = 4;

impl<B: ByteBufMut> Compressor<B> for Lz4 {
    type BufMut = BytesMut;
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        // Write uncompressed bytes into a temporary buffer
        let mut tmp = BytesMut::new();
        let res = f(&mut tmp)?;

        let mut encoder = EncoderBuilder::new()
            .level(COMPRESSION_LEVEL)
            .block_mode(BlockMode::Independent)
            .build(buf.writer())
            .context("Failed to compress lz4")?;

        io::copy(&mut tmp.reader(), &mut encoder).context("Failed to compress lz4")?;
        encoder.finish().1.context("Failed to compress lz4")?;

        Ok(res)
    }
}

impl<B: ByteBuf> Decompressor<B> for Lz4 {
    type Buf = Bytes;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_budget(buf, &mut DecodeBudget::default(), |decoded, _| f(decoded))
    }
}

impl<B: ByteBuf> BoundedDecompressor<B> for Lz4 {
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>,
    {
        budget.check()?;
        let input = buf.copy_to_bytes(buf.remaining());
        let mut decoder = Decoder::new(input.reader()).context("Failed to decompress lz4")?;
        let mut output = BoundedWriter::new(budget)?;
        let copied = io::copy(&mut decoder, &mut output).map(|_| ());
        let finished = decoder.finish().1;
        let mut decoded = output
            .finish(copied.and(finished))
            .context("Failed to decompress lz4")?;
        f(&mut decoded, budget)
    }
}

#[cfg(test)]
mod test {
    use crate::compression::Lz4;
    use crate::compression::{Compressor, Decompressor};
    use anyhow::Result;
    use bytes::BytesMut;
    use std::fmt::Write;
    use std::str;

    #[test]
    fn test_lz4() {
        let mut compressed = BytesMut::new();
        Lz4::compress(&mut compressed, |buf| -> Result<()> {
            buf.write_str("hello lz4").unwrap();
            Ok(())
        })
        .unwrap();

        Lz4::decompress(&mut compressed, |buf| -> Result<()> {
            let decompressed_str = str::from_utf8(buf.as_ref()).unwrap();
            assert_eq!(decompressed_str, "hello lz4");
            Ok(())
        })
        .unwrap();
    }
}
