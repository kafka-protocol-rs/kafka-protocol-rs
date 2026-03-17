use bytes::Bytes;
use proto_kafka::protocol::decode_request_header_from_buffer;

#[test]
fn request_header() {
    let mut bytes = Bytes::from(vec![
        0x00, 0x12, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x61, 0x64, 0x6d, 0x69, 0x6e,
        0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2d, 0x31, 0x00,
    ]);

    let res = decode_request_header_from_buffer(&mut bytes).unwrap();

    assert_eq!(res.request_api_key, 18);
    assert_eq!(res.request_api_version, 3);
    assert_eq!(res.client_id.unwrap().to_string(), "adminclient-1");
}
