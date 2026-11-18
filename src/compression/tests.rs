use super::*;
use crate::records::{DecodeBudget, DecodeLimitExceeded, DecodeLimits, DecodeResource};
use bytes::{Bytes, BytesMut};

fn encode<C: Compressor<BytesMut, BufMut = BytesMut>>(data: &[u8]) -> Bytes {
    let mut encoded = BytesMut::new();
    C::compress(&mut encoded, |output| {
        output.extend_from_slice(data);
        Ok(())
    })
    .unwrap();
    encoded.freeze()
}

fn decode<C: BoundedDecompressor<Bytes, Buf = Bytes>>(
    mut input: Bytes,
    budget: &mut DecodeBudget,
) -> Result<Bytes> {
    C::decompress_with_budget(&mut input, budget, |output, _| Ok(output.clone()))
}

fn budget(bytes: usize) -> DecodeBudget {
    DecodeBudget::new(DecodeLimits {
        decoded_bytes: bytes,
        ..DecodeLimits::default()
    })
}

fn boundaries<C>()
where
    C: Compressor<BytesMut, BufMut = BytesMut> + BoundedDecompressor<Bytes, Buf = Bytes>,
{
    // Crosses Snappy's encoder block boundary, with a deliberately small policy.
    let data = vec![b'a'; 65_537];
    let encoded = encode::<C>(&data);
    for limit in [0, data.len() - 1, data.len(), data.len() + 1] {
        let mut budget = budget(limit);
        if limit < data.len() {
            let error = C::decompress_with_budget(
                &mut encoded.clone(),
                &mut budget,
                |_, _| -> Result<()> { panic!("callback ran after exceeding output limit") },
            )
            .unwrap_err();
            let limit_error = error.downcast_ref::<DecodeLimitExceeded>().unwrap();
            assert_eq!(limit_error.resource, DecodeResource::DecodedBytes);
            assert_eq!(limit_error.limit, limit);
            // A failed scope must reject before consuming any subsequent input.
            let mut next = encoded.clone();
            assert!(C::decompress_with_budget(&mut next, &mut budget, |_, _| Ok(())).is_err());
            assert_eq!(next, encoded);
        } else {
            assert_eq!(
                decode::<C>(encoded.clone(), &mut budget).unwrap().as_ref(),
                data
            );
            assert_eq!(budget.consumed(DecodeResource::DecodedBytes), data.len());
        }
    }

    let mut shared = budget(2 * data.len() - 1);
    decode::<C>(encoded.clone(), &mut shared).unwrap();
    assert!(decode::<C>(encoded, &mut shared)
        .unwrap_err()
        .is::<DecodeLimitExceeded>());
    assert_eq!(
        decode::<C>(encode::<C>(&[]), &mut budget(0)).unwrap(),
        Bytes::new()
    );
}

#[cfg(any(
    feature = "gzip",
    feature = "lz4",
    feature = "zstd",
    feature = "snappy"
))]
fn truncated<C>()
where
    C: Compressor<BytesMut, BufMut = BytesMut> + BoundedDecompressor<Bytes, Buf = Bytes>,
{
    let data = b"completion checks must run even at exactly the output limit";
    let mut input = encode::<C>(data);
    input.truncate(input.len() - 1);
    let error =
        C::decompress_with_budget(&mut input, &mut budget(data.len()), |_, _| -> Result<()> {
            panic!("callback ran on truncated input")
        })
        .unwrap_err();
    assert!(!error.is::<DecodeLimitExceeded>());
}

#[test]
fn none_boundaries() {
    boundaries::<None>();
}
#[cfg(feature = "gzip")]
#[test]
fn gzip_boundaries_and_completion() {
    boundaries::<Gzip>();
    truncated::<Gzip>();
}
#[cfg(feature = "lz4")]
#[test]
fn lz4_boundaries_and_completion() {
    boundaries::<Lz4>();
    truncated::<Lz4>();
}
#[cfg(feature = "zstd")]
#[test]
fn zstd_boundaries_and_completion() {
    boundaries::<Zstd>();
    truncated::<Zstd>();
}
#[cfg(feature = "snappy")]
#[test]
fn snappy_boundaries_and_completion() {
    boundaries::<Snappy>();
    truncated::<Snappy>();
}

#[cfg(feature = "snappy")]
#[test]
fn raw_snappy_and_large_xerial_block() {
    // Decoder accepts blocks larger than our encoder's preferred 32 KiB size.
    let data = vec![3; 65_537];
    let raw = snap::raw::Encoder::new().compress_vec(&data).unwrap();
    let mut framed = Vec::from(&b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01"[..]);
    framed.extend_from_slice(&(raw.len() as u32).to_be_bytes());
    framed.extend_from_slice(&raw);
    for input in [Bytes::from(raw), Bytes::from(framed)] {
        assert_eq!(
            decode::<Snappy>(input.clone(), &mut budget(data.len()))
                .unwrap()
                .as_ref(),
            data
        );
        assert!(decode::<Snappy>(input, &mut budget(data.len() - 1))
            .unwrap_err()
            .is::<DecodeLimitExceeded>());
    }
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_concatenated_frames_share_output_limit() {
    let mut frames = encode::<Zstd>(b"first").to_vec();
    frames.extend_from_slice(&encode::<Zstd>(b"second"));
    let frames = Bytes::from(frames);
    assert_eq!(
        decode::<Zstd>(frames.clone(), &mut budget(11)).unwrap(),
        "firstsecond"
    );
    assert!(decode::<Zstd>(frames, &mut budget(10))
        .unwrap_err()
        .is::<DecodeLimitExceeded>());
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_window_is_independent_of_output_limit() {
    use std::io::Write;
    let data = vec![7; 4096];
    // Streaming encoder: no content-size pledge, as with Java producers. Select
    // the level-22 window explicitly without needing an expensive encoder level.
    let mut encoder = ::zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
    encoder.window_log(27).unwrap();
    encoder.write_all(&data).unwrap();
    let input = Bytes::from(encoder.finish().unwrap());
    assert_eq!(
        decode::<Zstd>(input.clone(), &mut budget(data.len()))
            .unwrap()
            .as_ref(),
        data
    );
    let mut restricted = DecodeBudget::new(DecodeLimits {
        decoded_bytes: data.len(),
        zstd_window_log_max: 10,
        ..DecodeLimits::default()
    });
    assert!(decode::<Zstd>(input, &mut restricted).is_err());
}

#[cfg(feature = "gzip")]
#[test]
fn gzip_preserves_single_member_behavior_and_validates_checksum() {
    let first = encode::<Gzip>(b"first");
    let mut members = first.to_vec();
    members.extend_from_slice(&encode::<Gzip>(b"second"));
    assert!(decode::<Gzip>(Bytes::from(members), &mut budget(11)).is_err());
    let mut corrupt = first.to_vec();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    assert!(decode::<Gzip>(Bytes::from(corrupt), &mut budget(5)).is_err());
}
