use bytes::Bytes;
use proto_kafka::messages::ApiVersionsRequest;
use proto_kafka::protocol::Decodable;

#[test]
fn api_versions() {
    let bytes: [u8; 25] = [
        0x12, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2d, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x6a,
        0x61, 0x76, 0x61, 0x06, 0x32, 0x2e, 0x38, 0x2e, 0x30, 0x00,
    ];

    let res = ApiVersionsRequest::decode(&mut Bytes::from(bytes.to_vec()), 3).unwrap();
    assert_eq!(res.client_software_name.to_string(), "apache-kafka-java");
    assert_eq!(res.client_software_version.to_string(), "2.8.0");
}
