//! Implementation of [the Kafka wire protocol](https://kafka.apache.org/protocol.html) in Rust.
//!
//! This library follows the approach of the Kafka project and generates the protocol through
//! [schema defined in JSON](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message).
//! This ensures not only compatibility, but easy synchronization when new features are added
//! upstream.
//!
//! The goal of this project is to provide 100% coverage of the Kafka API, as well as basic
//! utilities for working with the protocol, including compression and record serialization.
//!
//! # Messages
//! The [`messages`] module contains the generated request and response structs. These structs allow
//! easy serialization and deserialization via [`bytes::Bytes`].
//!
//! ```rust
//! use bytes::BytesMut;
//! use proto_kafka::protocol::{encode_request_header_into_buffer, decode_request_header_from_buffer};
//! use proto_kafka::messages::RequestHeader;
//! use proto_kafka::protocol::{StrBytes, Encodable, Decodable};
//!
//! let mut request_header = RequestHeader::default();
//! request_header.correlation_id = 1;
//! request_header.client_id = Some(StrBytes::from_static_str("test-client"));
//! let mut buf = BytesMut::new();
//! encode_request_header_into_buffer(&mut buf, &request_header).unwrap();
//! assert_eq!(request_header, decode_request_header_from_buffer(&mut buf).unwrap());
//! ```
//! Note that every message implementation of [`Encodable::encode`](crate::protocol::Encodable::encode)
//! and [`Decodable::decode`](crate::protocol::Decodable::decode) requires a version to be provided
//! explicitly. This is because every  message contains *all* the fields that are valid for every
//! version. These fields are *not* marked [`Option`], which would represent nullability according
//! to a message's schema, but rather have a default value that represents a nil value. It is the
//! user's responsibility to ensure that only valid fields of the decoded message version are used.
//!
//! ## Sending a Request
//!
//! A request can be created by serializing a [`messages::RequestHeader`] and any given request
//! type to a [`bytes::Bytes`] buffer.
//!
//! ```rust
//! use proto_kafka::protocol::{StrBytes, Encodable, HeaderVersion, encode_request_header_into_buffer};
//! use bytes::{BytesMut, Bytes};
//! use proto_kafka::messages::{RequestHeader, ApiKey, ApiVersionsRequest};
//! # use std::error::Error;
//!
//! let mut buf = BytesMut::new();
//! let mut req_header = RequestHeader::default();
//! req_header.request_api_version = 3;
//! req_header.request_api_key = ApiKey::ApiVersions as i16;
//! req_header.client_id = Some(StrBytes::from_static_str("example"));
//! encode_request_header_into_buffer(&mut buf, &req_header).unwrap();
//! let mut api_versions_req = ApiVersionsRequest::default();
//! api_versions_req.client_software_version = StrBytes::from_static_str("1.0");
//! api_versions_req.client_software_name = StrBytes::from_static_str("example-client");
//! api_versions_req.encode(&mut buf, req_header.request_api_version);
//!
//! # fn send_request(buf: &[u8]) -> Result<(), Box<dyn Error>> {
//! #     Ok(())
//! # }
//!
//! // implemented elsewhere
//! send_request(&buf[..]);
//! ```
//!
//! ## Deserializing an Unknown Request
//!
//! The [`messages`] module provides the enums [`messages::RequestKind`], [`messages::ResponseKind`],
//! and [`messages::ApiKey`] for matching on unknown requests and responses.
//!
//! A simple example for decoding an unknown message encoded in `buf`:
//! ```
//! # #[cfg(feature = "messages_enums")]
//! # {
//! use proto_kafka::messages::{RequestHeader, ApiVersionsRequest, ApiKey, RequestKind};
//! use proto_kafka::protocol::{Encodable, Decodable, StrBytes, HeaderVersion, decode_request_header_from_buffer, encode_request_header_into_buffer};
//! use bytes::{BytesMut, Buf};
//! use std::convert::TryFrom;
//! use proto_kafka::protocol::buf::ByteBuf;
//! # let mut buf = BytesMut::new();
//! # let mut req_header = RequestHeader::default();
//! # req_header.request_api_version = 3;
//! # req_header.request_api_key = ApiKey::ApiVersions as i16;
//! # req_header.client_id = Some(StrBytes::from_static_str("example"));
//! # encode_request_header_into_buffer(&mut buf, &req_header).unwrap();
//! # let mut api_versions_req = ApiVersionsRequest::default();
//! # api_versions_req.client_software_version = StrBytes::from_static_str("1.0");
//! # api_versions_req.client_software_name = StrBytes::from_static_str("example-client");
//! # api_versions_req.encode(&mut buf, 3).unwrap();
//!
//!
//! let header = decode_request_header_from_buffer(&mut buf).unwrap();
//! let api_key = ApiKey::try_from(header.request_api_key).unwrap();
//! let req = match api_key {
//!     ApiKey::ApiVersions => RequestKind::ApiVersions(ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap()),
//!     _ => panic!("Unsupported API key"),
//! };
//!
//! // match on enum elsewhere and do work
//! match req {
//!     RequestKind::ApiVersions(req) => {
//!         assert_eq!(req.client_software_name.to_string(), "example-client".to_string());
//!     }
//!     _ => panic!()
//! }
//! # }
//! ```
#![deny(missing_docs)]
// Display required features for items when rendering for docs.rs
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod compression;
pub mod error;
#[allow(clippy::all)]
pub mod messages;
pub mod protocol;
pub mod records;

pub use error::ResponseError;
pub use indexmap;