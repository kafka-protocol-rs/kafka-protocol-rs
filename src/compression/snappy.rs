use anyhow::{Context, Result};
use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use snap::raw::*;

use crate::protocol::buf::{ByteBuf, ByteBufMut};

use super::{Compressor, Decompressor};

/// Kafka variant of the snappy compression algorithm. See
/// https://github.com/xerial/snappy-java?tab=readme-ov-file#compatibility-notes for notes about
/// the difference from standard snappy.
/// See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
/// for more information about compression.
pub struct Snappy;

// See https://github.com/xerial/snappy-java/blob/98e66a1d02d022d333c20ffa6574a72e9fcfb165/src/main/java/org/xerial/snappy/SnappyOutputStream.java#L64
const DEFAULT_BLOCK_SIZE: usize = 32 * 1024;

const LENGTH_FIELD_LENGTH: usize = std::mem::size_of::<u32>();

impl<B: ByteBufMut> Compressor<B> for Snappy {
    type BufMut = BytesMut;
    fn compress<R, F>(compressed: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        let mut uncompressed = BytesMut::new();
        let res = f(&mut uncompressed)?;
        // See https://github.com/xerial/snappy-java?tab=readme-ov-file#compatibility-notes
        compressed.put_slice(MAGIC_HEADER);

        while uncompressed.has_remaining() {
            let uncompressed_block_size =
                std::cmp::min(uncompressed.remaining(), DEFAULT_BLOCK_SIZE);
            let estimated_compressed_block_size = max_compress_len(uncompressed_block_size);

            let length_gap = compressed.put_gap(LENGTH_FIELD_LENGTH);

            let block_offset = compressed.offset();
            let compressed_block = compressed.put_gap(estimated_compressed_block_size);

            let bytes_written = Encoder::new()
                .compress(
                    &uncompressed.split_to(uncompressed_block_size),
                    compressed.gap_buf(compressed_block),
                )
                .context("failed to compress snappy block")?;
            // Truncate down to the final compressed size
            compressed.seek(block_offset + bytes_written);

            let mut num_written_buf = compressed.gap_buf(length_gap);
            num_written_buf.put_u32(bytes_written as u32);
        }
        Ok(res)
    }
}

const MAGIC_HEADER: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

impl<B: ByteBuf> Decompressor<B> for Snappy {
    type Buf = Bytes;
    fn decompress<R, F>(compressed: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        // See https://github.com/xerial/snappy-java?tab=readme-ov-file#compatibility-notes
        if !compressed.has_remaining() {
            anyhow::bail!("expected some bytes in snappy stream");
        }

        // We fall back to non-Kafka "raw" snappy compression if the magic header is not present
        // just like the [Java implementation](https://github.com/xerial/snappy-java/blob/48b31663c8b9d01d758368a26416ef6194045c5f/src/main/java/org/xerial/snappy/SnappyInputStream.java#L114).
        //
        // We must peek at the magic bytes without consuming them, because
        // try_get_bytes advances the buffer. If the magic doesn't match,
        // the raw fallback needs the full original data.
        if compressed
            .try_peek_bytes(0..MAGIC_HEADER.len())
            .ok()
            .is_none_or(|magic| *magic != MAGIC_HEADER[..])
        {
            let compressed = compressed.copy_to_bytes(compressed.remaining());
            let actual_len = decompress_len(&compressed).context("failed to read snappy header")?;
            let mut tmp = BytesMut::zeroed(actual_len);
            Decoder::new()
                .decompress(&compressed, &mut tmp)
                .context("failed to decompress raw snappy bytes")?;

            return f(&mut tmp.into());
        }

        // Consume the magic header now that we know it matches
        let _ = compressed.try_get_bytes(MAGIC_HEADER.len());

        let mut uncompressed = BytesMut::new();
        while compressed.has_remaining() {
            let compressed_block_size = compressed
                .try_get_u32()
                .context("not enough bytes to read compressed block length")?
                as usize;
            let compressed_block = compressed
                .try_get_bytes(compressed_block_size)
                .context("not enough bytes for block")?;

            let uncompressed_block_length = decompress_len(&compressed_block)
                .context("failed to get snappy uncompressed length")?;
            let uncompressed_block_start = uncompressed.len();
            uncompressed.resize(
                uncompressed_block_start.saturating_add(uncompressed_block_length),
                0,
            );

            Decoder::new()
                .decompress(
                    &compressed_block,
                    &mut uncompressed[uncompressed_block_start..],
                )
                .context("failed to decompress snappy block")?;
        }

        f(&mut uncompressed.into())
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf as _, Bytes, BytesMut};
    use indexmap::IndexMap;

    use crate::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    use super::super::{Compressor as _, Decompressor as _};
    use super::Snappy;

    #[test]
    fn compression() {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: None,
            value: Some(Bytes::from_static(b"sdfdsf")),
            headers: IndexMap::default(),
        };

        // The module doesn't expose record encode/decode directly so we have to put everything
        // into a batch to compare the bytes
        let mut raw_bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut raw_bytes,
            vec![&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("should encode");

        // Chop off all the record batch bytes before the records
        raw_bytes.advance(61);

        let mut compressed = BytesMut::new();
        Snappy::compress(&mut compressed, |uncompressed| {
            std::mem::swap(uncompressed, &mut raw_bytes);
            Ok(())
        })
        .expect("should compress");

        // Some upstream snappy-compressed record batch bytes
        let expected_bytes = Bytes::from_static(
            b"\x82\x53\x4e\x41\x50\x50\x59\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x0f\x0d\x30\x18\x00\x00\x00\x01\x0csdfdsf\x00",
        );

        assert_eq!(expected_bytes, compressed);
    }

    #[test]
    fn decompression() {
        // Some upstream kafka snappy-compressed record batch bytes
        let mut raw_bytes = Bytes::from_static(
            b"\x82\x53\x4e\x41\x50\x50\x59\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x0f\x0d\x30\x18\x00\x00\x00\x01\x0csdfdsf\x00",
        );
        let decompressed = Snappy::decompress(&mut raw_bytes, |buf| {
            let mut out = Bytes::new();
            std::mem::swap(buf, &mut out);
            Ok(out)
        })
        .expect("valid snappy");

        // The module doesn't expose record encode/decode directly so we have to put everything
        // into a batch to compare the bytes
        let mut expected_bytes = BytesMut::new();
        let expected_record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: None,
            value: Some(Bytes::from_static(b"sdfdsf")),
            headers: IndexMap::default(),
        };
        RecordBatchEncoder::encode(
            &mut expected_bytes,
            vec![&expected_record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("should encode");

        let mut expected_bytes = expected_bytes.freeze();
        // Chop off all the record batch bytes before the records
        expected_bytes.advance(61);
        assert_eq!(expected_bytes, decompressed);
    }

    #[test]
    fn decompression_fallback() {
        // Some pure snappy-compressed record batch bytes
        let mut raw_bytes = Bytes::from_static(b"\r0\x18\0\0\0\x01\x0csdfdsf\0");
        let decompressed = Snappy::decompress(&mut raw_bytes, |buf| {
            let mut out = Bytes::new();
            std::mem::swap(buf, &mut out);
            Ok(out)
        })
        .expect("valid snappy");

        // The module doesn't expose record encode/decode directly so we have to put everything
        // into a batch to compare the bytes
        let mut expected_bytes = BytesMut::new();
        let expected_record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: Default::default(),
            timestamp: Default::default(),
            key: None,
            value: Some(Bytes::from_static(b"sdfdsf")),
            headers: IndexMap::default(),
        };
        RecordBatchEncoder::encode(
            &mut expected_bytes,
            vec![&expected_record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("should encode");

        let mut expected_bytes = expected_bytes.freeze();
        // Chop off all the record batch bytes before the records
        expected_bytes.advance(61);
        assert_eq!(expected_bytes, decompressed);
    }

    /// Regression test: raw snappy data ≥16 bytes.
    ///
    /// Before the fix, `try_get_bytes(16)` consumed the first 16 bytes
    /// when checking for the Xerial magic header. If the magic didn't
    /// match (raw snappy), the fallback got truncated data and failed
    /// with "failed to decompress raw snappy bytes".
    ///
    /// The `decompression_fallback` test above uses only 13 bytes of
    /// compressed data, so `try_get_bytes(16)` failed (not enough bytes)
    /// and the bug was never triggered.
    #[test]
    fn decompression_fallback_large_payload() {
        // Create a payload large enough that raw-snappy-compressed output is ≥16 bytes.
        let original = b"Hello world! This is a test payload that is long enough to \
            produce more than sixteen bytes of snappy-compressed output, which triggers \
            the regression where try_get_bytes consumed the magic header probe bytes.";

        // Compress with raw snappy (no Xerial framing)
        let compressed_vec = snap::raw::Encoder::new()
            .compress_vec(original)
            .expect("raw snappy compress");
        assert!(
            compressed_vec.len() >= 16,
            "compressed payload must be ≥16 bytes to trigger the regression (got {})",
            compressed_vec.len()
        );

        let mut compressed = Bytes::from(compressed_vec);
        let decompressed = Snappy::decompress(&mut compressed, |buf| {
            let mut out = Bytes::new();
            std::mem::swap(buf, &mut out);
            Ok(out)
        })
        .expect("raw snappy decompression of ≥16 byte payload should succeed");

        assert_eq!(&decompressed[..], &original[..]);
    }
}
