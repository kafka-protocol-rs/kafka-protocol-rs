use anyhow::{ensure, Context, Result};
use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use snap::raw::*;

use crate::protocol::buf::{ByteBuf, ByteBufMut};

use super::{BoundedDecompressor, Compressor, Decompressor};
use crate::records::DecodeBudget;

/// Kafka variant of the snappy compression algorithm. See
/// <https://github.com/xerial/snappy-java?tab=readme-ov-file#compatibility-notes> for notes about
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

// Snappy copy tags produce at most 64 bytes from three encoded bytes. The other
// copy forms and literals have lower ratios. 22x (including the length prefix)
// is therefore a conservative format bound, not a workload compression policy.
fn checked_decompress_len(compressed: &[u8]) -> Result<usize> {
    let len = decompress_len(compressed).context("failed to read snappy header")?;
    ensure!(
        len <= compressed.len().saturating_mul(22),
        "snappy decoded length exceeds the format's maximum expansion"
    );
    Ok(len)
}

impl<B: ByteBuf> Decompressor<B> for Snappy {
    type Buf = Bytes;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_budget(buf, &mut DecodeBudget::default(), |decoded, _| f(decoded))
    }
}

impl<B: ByteBuf> BoundedDecompressor<B> for Snappy {
    fn decompress_with_budget<R, F>(buf: &mut B, budget: &mut DecodeBudget, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf, &mut DecodeBudget) -> Result<R>,
    {
        budget.check()?;
        let input = buf.copy_to_bytes(buf.remaining());
        ensure!(!input.is_empty(), "expected some bytes in snappy stream");

        // Preflight every block before allocating any output. A second pass avoids
        // retaining a separate collection of attacker-controlled block descriptors.
        let mut total = 0usize;
        visit_blocks(&input, |block| {
            let len = checked_decompress_len(block)?;
            budget.charge(crate::records::DecodeResource::DecodedBytes, len)?;
            total = total
                .checked_add(len)
                .context("snappy output length overflow")?;
            Ok(())
        })?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(total)
            .context("failed to allocate snappy output")?;
        output.resize(total, 0);
        let mut offset = 0;
        let mut decoder = Decoder::new();
        visit_blocks(&input, |block| {
            let len = checked_decompress_len(block)?;
            let end = offset + len;
            let written = decoder
                .decompress(block, &mut output[offset..end])
                .context("failed to decompress snappy block")?;
            ensure!(written == len, "snappy decoded length mismatch");
            offset = end;
            Ok(())
        })?;
        f(&mut output.into(), budget)
    }
}

fn visit_blocks(input: &[u8], mut f: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    // Preserve Java-compatible raw Snappy fallback when the Xerial header is absent.
    let Some(mut remaining) = input.strip_prefix(MAGIC_HEADER) else {
        return f(input);
    };
    while !remaining.is_empty() {
        let len = u32::from_be_bytes(
            remaining
                .get(..4)
                .context("not enough bytes to read compressed block length")?
                .try_into()?,
        ) as usize;
        remaining = &remaining[4..];
        let block = remaining.get(..len).context("not enough bytes for block")?;
        f(block)?;
        remaining = &remaining[len..];
    }
    Ok(())
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
    fn rejects_impossible_declared_lengths() {
        // A 4096-byte declaration without any tags cannot be a valid block.
        let raw = [0x80, 0x20];
        let mut xerial = Vec::from(super::MAGIC_HEADER.as_slice());
        xerial.extend_from_slice(&(raw.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&raw);
        for encoded in [raw.to_vec(), xerial] {
            let error = Snappy::decompress(&mut Bytes::from(encoded), |_| Ok(())).unwrap_err();
            assert!(error.to_string().contains("maximum expansion"));
        }
    }

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
