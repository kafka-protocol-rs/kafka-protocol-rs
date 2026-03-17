use crate::error::{Result, ResultExt};
use crate::protocol::buf::{ByteBuf, ByteBufMut};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Compressor, Decompressor};

/// Gzip compression algorithm. See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
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
        let mut tmp = BytesMut::new().writer();
        // Allocate a temporary buffer to hold the uncompressed bytes
        let buf = buf.copy_to_bytes(buf.remaining());
        zstd::stream::copy_decode(buf.reader(), &mut tmp).context("Failed to decompress zstd")?;

        f(&mut tmp.into_inner().into())
    }
}

#[cfg(test)]
mod test {
    use crate::compression::Zstd;
    use crate::compression::{Compressor, Decompressor};
    use crate::error::Result;
    use bytes::BytesMut;
    use std::fmt::Write;
    use std::str;
    use zstd::zstd_safe::WriteBuf;

    #[test]
    fn test_zstd() {
        let mut compressed = BytesMut::new();
        Zstd::compress(&mut compressed, |buf| -> Result<()> {
            buf.write_str("hello zstd").unwrap();
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
