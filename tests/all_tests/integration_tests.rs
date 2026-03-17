use crate::all_tests::common::*;
use proto_kafka::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader};

#[test]
fn get_api_versions() {
    let container = start_kafka();
    let mut socket = connect_to_kafka(&container);

    let version = 2;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(version);

    let request = ApiVersionsRequest::default();
    send_request(&mut socket, header, request);

    let result: ApiVersionsResponse = receive_response(&mut socket, version).1;

    assert_eq!(result.error_code, 0);
    // On this specific version of kafka
    assert_eq!(result.api_keys.len(), 55);
}
