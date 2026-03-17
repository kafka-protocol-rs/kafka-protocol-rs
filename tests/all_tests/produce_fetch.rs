use std::{net::TcpStream, time::Duration};

use bytes::{Bytes, BytesMut};
use proto_kafka::{
    messages::{
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
        produce_request::{PartitionProduceData, TopicProduceData},
        ApiKey, CreateTopicsRequest, CreateTopicsResponse, FetchRequest, FetchResponse,
        ProduceRequest, ProduceResponse, RequestHeader, TopicName,
    },
    protocol::StrBytes,
    records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    },
};

use crate::all_tests::common::*;

#[test]
fn record_batch_produce_fetch() {
    let topic_name = TopicName(StrBytes::from_static_str("record_batch_produce_fetch"));

    let container = start_kafka();
    let mut socket = connect_to_kafka(&container);

    let records = vec![
        new_record(0, true),
        new_record(1, true),
        new_record(2, true),
    ];

    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut encoded,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
        Some(compress_record_batch_data),
    )
    .unwrap();

    create_topic(topic_name.clone(), &mut socket);

    // Sometimes (rarely) the produce request will fail with error code 6 (NOT_LEADER_OR_FOLLOWER)
    // Hopefully a 1s sleep will prevent that
    std::thread::sleep(Duration::from_secs(1));

    produce_records(topic_name.clone(), 9, encoded.freeze(), &mut socket);
    fetch_records(
        topic_name.clone(),
        12,
        records,
        (2, Compression::None),
        &mut socket,
    );
}

fn create_topic(topic_name: TopicName, socket: &mut TcpStream) {
    let version = 7;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::CreateTopics as i16)
        .with_request_api_version(version);

    let request = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![CreatableTopic::default()
            .with_num_partitions(1)
            .with_name(topic_name.clone())
            .with_replication_factor(1)]);

    send_request(socket, header, request);
    let result: CreateTopicsResponse = receive_response(socket, version).1;

    assert_eq!(result.throttle_time_ms, 0, "response throttle time");

    let topic = result.topics.first().unwrap();

    assert_eq!(topic.name, topic_name, "topic name");
    assert_eq!(topic.error_code, 0, "topic error code");
    assert_eq!(topic.error_message, None, "topic error message");
}

fn produce_records(topic_name: TopicName, version: i16, records: Bytes, socket: &mut TcpStream) {
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(version);

    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(topic_name.clone())
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records))])]);

    send_request(socket, header, request);

    let result: ProduceResponse = receive_response(socket, version).1;

    assert_eq!(result.throttle_time_ms, 0, "produce response throttle time");

    let topic_response = result.responses.first().unwrap();
    assert_eq!(
        topic_response.name, topic_name,
        "produce response partition index"
    );
    let partition_response = topic_response.partition_responses.first().unwrap();

    assert_eq!(
        partition_response.index, 0,
        "produce response partition index"
    );
    assert_eq!(
        partition_response.error_message, None,
        "produce recponse partition error message"
    );
    assert_eq!(
        partition_response.error_code, 0,
        "produce response partition error code"
    );
}

fn fetch_records(
    topic_name: TopicName,
    api_version: i16,
    expected: Vec<Record>,
    (expected_record_version, expected_compression): (i8, Compression),
    socket: &mut TcpStream,
) {
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(api_version);

    let request = FetchRequest::default().with_topics(vec![FetchTopic::default()
        .with_topic(topic_name.clone())
        .with_partitions(vec![FetchPartition::default()
            .with_partition(0)
            .with_fetch_offset(0)])]);

    send_request(socket, header, request);

    let result: FetchResponse = receive_response(socket, api_version).1;

    assert_eq!(result.throttle_time_ms, 0, "fetch response throttle time");
    assert_eq!(result.error_code, 0, "fetch response error code");

    let topic_response = result.responses.first().unwrap();

    assert_eq!(
        topic_response.topic, topic_name,
        "fetch response topic name"
    );

    let partition_response = topic_response.partitions.first().unwrap();

    assert_eq!(
        partition_response.partition_index, 0,
        "fetch response partition index"
    );
    assert_eq!(
        partition_response.error_code, 0,
        "fetch resopnse partition error code"
    );

    let mut fetched_records = partition_response.records.clone().unwrap();
    let decoded_records =
        RecordBatchDecoder::decode_all(&mut fetched_records).expect("records should decode");
    if expected_record_version == 2 {
        assert_eq!(1, decoded_records.len());
    } else {
        assert_eq!(expected.len(), decoded_records.len());
    }
    for decoded in &decoded_records {
        assert_eq!(expected_record_version, decoded.version);
        assert_eq!(expected_compression, decoded.compression);
    }

    eprintln!("{expected:#?}");
    eprintln!("{fetched_records:#?}");

    assert_eq!(
        expected,
        decoded_records
            .into_iter()
            .flat_map(|r| r.records)
            .collect::<Vec<_>>()
    );
}

fn new_record(offset: i64, v2: bool) -> Record {
    Record {
        transactional: false,
        control: false,
        partition_leader_epoch: if v2 { 0 } else { -1 },
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        timestamp: offset,
        sequence: if v2 { offset as _ } else { -1 },
        offset,
        key: Some(format!("key{offset}").into()),
        value: Some(format!("value{offset}").into()),
        headers: Default::default(),
    }
}

fn compress_record_batch_data(
    src: &mut bytes::BytesMut,
    dest: &mut BytesMut,
    compression: Compression,
) -> proto_kafka::error::Result<()> {
    match compression {
        Compression::None => {
            dest.extend_from_slice(src.as_ref());
            Ok(())
        }
        _ => {
            panic!("Compression not implemented")
        }
    }
}
