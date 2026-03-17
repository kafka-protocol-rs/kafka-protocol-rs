#[cfg(feature = "client")]
mod client_tests {
    use bytes::Bytes;
    use proto_kafka::records::Compression;
    use proto_kafka::{messages::FetchResponse, protocol::Decodable, records::RecordBatchDecoder};

    const HEADERS: [u8; 45] = [
        // Throttle time
        0x00, 0x00, 0x00, 0x00, // Number of topics
        0x00, 0x00, 0x00, 0x01, // Topic name: hello
        0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // Number of partitions
        0x00, 0x00, 0x00, 0x01, // Partition 0
        0x00, 0x00, 0x00, 0x00, // Error
        0x00, 0x00, // High Watermark Offset
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Last Stable Offset
        0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, // Aborted transactions
        0x00, 0x00, 0x00, 0x00,
    ];

    const FIRST_RECORD: [u8; 79] = [
        // First offset
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Record Batch Size
        0x0, 0x0, 0x0, 0x43, // Partition Leader Epoch
        0x0, 0x0, 0x0, 0x0, // Magic byte
        0x2, // CRC
        0x73, 0x6d, 0x29, 0x7b, // Attributes
        0x0, 0b00000000, // Last offset delta
        0x0, 0x0, 0x0, 0x3, // First timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Max timestamp
        0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, // Producer ID
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Producer epoch
        0x0, 0x0, // First sequence
        0x0, 0x0, 0x0, 0x0, // Number of records
        0x0, 0x0, 0x0, 0x1,  // Size
        0x22, // Attributes
        0x1,  // Timestamp delta
        0xd0, 0xf, // Offset delta
        0x2, // Key
        0xa, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // Value
        0xa, 0x77, 0x6f, 0x72, 0x6c, 0x64, // Header
        0x0,
    ];

    const SECOND_RECORD: [u8; 79] = [
        // First offset
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Record Batch Size
        0x0, 0x0, 0x0, 0x43, // Partition Leader Epoch
        0x0, 0x0, 0x0, 0x0, // Magic byte
        0x2, // CRC
        0x04, 0xb9, 0x4d, 0x7b, // Attributes
        0x0, 0b00000000, // Last offset delta
        0x0, 0x0, 0x0, 0x3, // First timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Max timestamp
        0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, // Producer ID
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Producer epoch
        0x0, 0x0, // First sequence
        0x0, 0x0, 0x0, 0x0, // Number of records
        0x0, 0x0, 0x0, 0x1,  // Size
        0x22, // Attributes
        0x1,  // Timestamp delta
        0xd0, 0xf, // Offset delta
        0x2, // Key
        0xa, 0x68, 0x69, 0x69, 0x69, 0x69, // Value
        0xa, 0x62, 0x79, 0x65, 0x65, 0x65, // Header
        0x0,
    ];

    #[test]
    fn first_record() {
        let mut res = vec![];
        res.extend_from_slice(&HEADERS[..]);
        res.extend_from_slice(&[0x00, 0x00, 0x00, 0x4f]);
        res.extend_from_slice(&FIRST_RECORD[..]);

        let res = FetchResponse::decode(&mut Bytes::from(res), 4).unwrap();
        assert_eq!(res.responses.len(), 1);

        for topic in res.responses {
            assert_eq!(topic.topic.0.to_string(), "hello");
            assert_eq!(topic.partitions.len(), 1);
            for partition in topic.partitions {
                assert_eq!(partition.partition_index, 0);
                assert_eq!(partition.error_code, 0);
                assert_eq!(partition.aborted_transactions.as_ref().unwrap().len(), 0);

                let mut records = partition.records.unwrap();
                let decoded = RecordBatchDecoder::decode_with_custom_compression(
                    &mut records,
                    Some(decompress_record_batch_data),
                )
                .unwrap();
                assert_eq!(decoded.compression, Compression::None);
                assert_eq!(decoded.version, 2);
                assert_eq!(decoded.records.len(), 1);
                for record in decoded.records {
                    assert_eq!(
                        String::from_utf8(record.key.unwrap().to_vec()).unwrap(),
                        "hello"
                    );
                    assert_eq!(
                        String::from_utf8(record.value.unwrap().to_vec()).unwrap(),
                        "world"
                    );
                }
            }
        }
    }

    #[test]
    fn second_record() {
        let mut res = vec![];
        res.extend_from_slice(&HEADERS[..]);
        res.extend_from_slice(&[0x00, 0x00, 0x00, 0x4f]);
        res.extend_from_slice(&SECOND_RECORD[..]);

        let res = FetchResponse::decode(&mut Bytes::from(res), 4).unwrap();
        assert_eq!(res.responses.len(), 1);

        for topic in res.responses {
            assert_eq!(topic.topic.0.to_string(), "hello");
            assert_eq!(topic.partitions.len(), 1);
            for partition in topic.partitions {
                assert_eq!(partition.partition_index, 0);
                assert_eq!(partition.error_code, 0);
                assert_eq!(partition.aborted_transactions.as_ref().unwrap().len(), 0);

                let mut records = partition.records.unwrap();
                let decoded = RecordBatchDecoder::decode(&mut records).unwrap();
                assert_eq!(decoded.compression, Compression::None);
                assert_eq!(decoded.version, 2);
                assert_eq!(decoded.records.len(), 1);
                for record in decoded.records {
                    assert_eq!(
                        String::from_utf8(record.key.unwrap().to_vec()).unwrap(),
                        "hiiii"
                    );
                    assert_eq!(
                        String::from_utf8(record.value.unwrap().to_vec()).unwrap(),
                        "byeee"
                    );
                }
            }
        }
    }

    #[test]
    fn multiple_records() {
        let mut res = vec![];
        res.extend_from_slice(&HEADERS[..]);
        res.extend_from_slice(&[0x00, 0x00, 0x00, 0x4f]);
        res.extend_from_slice(&FIRST_RECORD[..]);
        res.extend_from_slice(&SECOND_RECORD[..]);

        let res = FetchResponse::decode(&mut Bytes::from(res), 4).unwrap();
        assert_eq!(res.responses.len(), 1);

        for topic in res.responses {
            assert_eq!(topic.topic.0.to_string(), "hello");
            assert_eq!(topic.partitions.len(), 1);
            for partition in topic.partitions {
                assert_eq!(partition.partition_index, 0);
                assert_eq!(partition.error_code, 0);
                assert_eq!(partition.aborted_transactions.as_ref().unwrap().len(), 0);

                let mut records = partition.records.unwrap();
                let decoded = RecordBatchDecoder::decode_all(&mut records).unwrap();
                let [decoded] = decoded.as_slice() else {
                    panic!("expected exactly one record");
                };
                assert_eq!(decoded.compression, Compression::None);
                assert_eq!(decoded.version, 2);
                assert_eq!(decoded.records.len(), 1);
            }
        }
    }

    fn decompress_record_batch_data(
        compressed_buffer: &mut bytes::Bytes,
        compression: Compression,
    ) -> proto_kafka::error::Result<Bytes> {
        match compression {
            Compression::None => Ok(compressed_buffer.to_vec().into()),
            _ => {
                panic!("Compression not implemented")
            }
        }
    }
}
