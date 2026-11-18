use crate::protocol::buf::{ByteBuf, ByteBufMut};
use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::bounded::BoundedWriter;
use super::{BoundedDecompressor, Compressor, Decompressor};
use crate::records::DecodeBudget;

/// Zstd compression algorithm. See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
/// for more information.
pub struct Zstd;

const COMPRESSION_LEVEL: i32 = 3;

impl<B: ByteBufMut> Compressor<B> for Zstd {
    type BufMut = BytesMut;
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        // Write uncompressed bytes into a temporary buffer
        let mut tmp = BytesMut::new();
        let res = f(&mut tmp)?;

        // Compress directly into the target buffer
        zstd::stream::copy_encode(tmp.reader(), buf.writer(), COMPRESSION_LEVEL)
            .context("Failed to compress zstd")?;
        Ok(res)
    }
}

impl<B: ByteBuf> Decompressor<B> for Zstd {
    type Buf = Bytes;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_budget(buf, &mut DecodeBudget::default(), |decoded, _| f(decoded))
    }
}

impl<B: ByteBuf> BoundedDecompressor<B> for Zstd {
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>,
    {
        budget.check()?;
        let input = buf.copy_to_bytes(buf.remaining());
        let mut decoder = zstd::stream::read::Decoder::new(input.reader())
            .context("Failed to create zstd decoder")?;
        decoder
            .window_log_max(budget.limits().zstd_window_log_max)
            .context("Failed to configure zstd window limit")?;
        let mut output = BoundedWriter::new(budget)?;
        let result = std::io::copy(&mut decoder, &mut output).map(|_| ());
        let mut decoded = output.finish(result).context("Failed to decompress zstd")?;
        f(&mut decoded, budget)
    }
}

#[cfg(test)]
mod test {
    use crate::compression::Zstd;
    use crate::compression::{Compressor, Decompressor};
    use anyhow::Result;
    use bytes::BytesMut;
    use std::fmt::Write;
    use std::str;
    use zstd::zstd_safe::WriteBuf;

    #[test]
    fn test_zstd() {
        let mut compressed = BytesMut::new();
        Zstd::compress(&mut compressed, |buf| -> Result<()> {
            buf.write_str("hello zstd")?;
            Ok(())
        })
        .unwrap();

        Zstd::decompress(&mut compressed, |buf| -> Result<()> {
            let decompressed_str = str::from_utf8(buf.as_slice())?;
            assert_eq!(decompressed_str, "hello zstd");
            Ok(())
        })
        .unwrap();
    }
}
