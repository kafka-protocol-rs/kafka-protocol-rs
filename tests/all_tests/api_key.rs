use proto_kafka::messages::ApiKey;

#[test]
fn api_key_iter() {
    assert_eq!(ApiKey::iter().count(), 87);
    assert_eq!(ApiKey::iter().next().unwrap(), ApiKey::Produce);
}
