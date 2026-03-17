# proto-kafka [![Build](https://github.com/aovestdipaperino/proto-kafka/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/aovestdipaperino/proto-kafka/actions/workflows/build-and-test.yml) [![crates.io](https://img.shields.io/crates/v/proto-kafka.svg)](https://crates.io/crates/proto-kafka) [![docs.rs](https://img.shields.io/docsrs/proto-kafka)](https://docs.rs/proto-kafka)

> **This crate is a derived work from [`kafka-protocol`](https://crates.io/crates/kafka-protocol)
> ([tychedelia/kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs)),
> which appears to be no longer actively maintained.
> `proto-kafka` is published as a separate crate to provide continued maintenance, bug fixes,
> and improvements while preserving full API compatibility.**

Rust implementation of the [Kafka wire protocol](https://kafka.apache.org/protocol.html).

Unlike other Kafka protocol implementations, this project uses code generation to cover the entire Kafka API surface,
including different protocol versions. See [Kafka's repo](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message)
for an example of protocol schema.

## Versioning

Protocol messages are generated against a recent stable Kafka release, currently [4.1.0](https://github.com/apache/kafka/releases/tag/4.1.0).

Although the Kafka protocol remains relatively stable and strives to be backwards compatible, new fields are occasionally
added. In order to ensure forward compatibility with the protocol, this crate marks all exported items as `#[non-exhaustive]`.
Protocol messages can be constructed using `Default::default` and updated with builder style methods.

## Working with messages

Using `Default::default`:
```rust
use proto_kafka::messages::{ApiKey, MetadataRequest, RequestHeader};
use proto_kafka::protocol::StrBytes;

let mut header = RequestHeader::default();
header.client_id = Some(StrBytes::from_static_str("my-client"));
header.request_api_key = ApiKey::Metadata as i16;
header.request_api_version = 12;

let mut request = MetadataRequest::default();
request.topics = None;
request.allow_auto_topic_creation = true;
```

Using builder style methods:
```rust
use proto_kafka::messages::{ApiKey, MetadataRequest, RequestHeader};
use proto_kafka::protocol::StrBytes;

let header = RequestHeader::default()
    .with_client_id(Some(StrBytes::from_static_str("my-client")))
    .with_request_api_key(ApiKey::Metadata as i16)
    .with_request_api_version(12);

let request = MetadataRequest::default()
    .with_topics(None)
    .with_allow_auto_topic_creation(true);
```
### Serialization

Once a message has been created, it can be serialized using `Encodable`, writing
the struct to a provided `bytes::BytesMut`. The API version for the given message
matching the version specified in the request header must be provided.

```rust
use bytes::BytesMut;
use proto_kafka::messages::MetadataRequest;
use proto_kafka::protocol::Encodable;

let mut bytes = BytesMut::new();
let request = MetadataRequest::default();
request.encode(&mut bytes, 12).unwrap();
```

### Deserialization

Messages can be decoded using `Decodable` and providing the matching API version from their
corresponding request.

```rust
use bytes::Bytes;
use proto_kafka::messages::ApiVersionsRequest;
use proto_kafka::protocol::Decodable;

let bytes: [u8; 25] = [
        0x12, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2d,
        0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x6a, 0x61,
        0x76, 0x61, 0x06, 0x32, 0x2e, 0x38, 0x2e, 0x30,
        0x00
];

let res = ApiVersionsRequest::decode(&mut Bytes::from(bytes.to_vec()), 3).unwrap();
```

### Development

Run `cargo run -p protocol_codegen` in the root path of this repo to generate/update the Rust codes via the latest Kafka
protocol schema.

## Attribution

**This crate is derived from [`kafka-protocol`](https://crates.io/crates/kafka-protocol)
([tychedelia/kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs)).**
Originally implemented by [@Diggsey](https://github.com/Diggsey) in a minimal Kafka client
implementation [Franz](https://github.com/Diggsey/franz).

## MSRV

Any release is guaranteed to be compatible with the last 3 rust releases.

In practice we will likely be compatible with many more releases, and we try to avoid bumping the version without a good reason.
