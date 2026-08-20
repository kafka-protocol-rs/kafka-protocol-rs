# Changelog

## 0.18.0

### Breaking changes ⚠️

* feat: add KIP-534 Batch attribute `delete_horizon` by @zac-torkelson in https://github.com/kafka-protocol-rs/kafka-protocol-rs/pull/157

### Fixes

* fix: snappy decompress corrupts data when Xerial magic check fails by @StevenLeRoux in https://github.com/kafka-protocol-rs/kafka-protocol-rs/pull/149
* fix: decode/encode record timestampDelta as VarLong (i64) instead of VarInt (i32) by @hadiezatpanah in https://github.com/ kafka-protocol-rs/kafka-protocol-rs/pull/159

### Features

* Make ResponseError fn const by @allevo in https://github.com/kafka-protocol-rs/kafka-protocol-rs/pull/153
* Expose batch decode info by @jakubdyszkiewicz in https://github.com/kafka-protocol-rs/kafka-protocol-rs/pull/150

## 0.17.0

- Breaking change:
  - change snappy compression to:
    - encode using the upstream java algorithm
    - and decode by autodetecting the upstream java algorithm or true snappy algorithm (previous kafka-protocol-rs encoding implementation)
  - This change makes it possible to decompress bytes compressed by the upstream java
- Fix ApiKey::iter which would terminate early in 0.16.0

## 0.16.0

- Added support for kafka 4.0
  - deprecated message types were removed in kafka 4.0 and those types have also been removed from this crate.
  - removed support for message sets v0 and v1

## 0.15.1

- Ensure bytes dependency is at least 0.10.1

## 0.15.0

- `Records::encode`/`Records::decode` no longer has unneeded trait bounds.
- `ByteBuf::try_get_*` methods that already exist on `Buf` are removed in favor of the `Buf` method

## v0.14.0

- `Records::encode`/`Records::decode` is now reverted back to their original API, instead `encode_with_custom_compression` and `decode_with_custom_compression` is provided for custom compression.
- `ApiKey` variants are renamed to remove the `Key` suffix.
- Add `iterate_all` and `valid_versions` methods to `ApiKey`
- Implement `PartialEq` and `Display` for `VersionRange`

## v0.13.0

- All "map" types in the protocol that were previously of type `IndexMap<K, V>` are now of type `Vec<V>`, the value of `K` is stored as a field within `V`.
  - This was done to resolve <https://github.com/tychedelia/kafka-protocol-rs/issues/84> and improve decoding speed.
  - If you were previously calling `.get()` on the map, the best way to migrate is to refactor your code to avoid the need to lookup by iterating over the items in the response instead.
  - Alternatively, you could replace `responses.get(name)` with something like `responses.iter().find(|x| x.name == name)` to achieve the same result. But note that this access is now O(N) instead of O(1).
  - Alternatively, you could use an intermediate hashmap before converting to a Vec to retain the O(1) lookup.
- Update protocol to kafka 3.8.0
- The Debug impl for new type wrappers now passes directly to the inner type.
  The full list of new type wrappers is BrokerId, GroupId, ProducerId, TopicName and TransactionalId.
  For example GroupId was previously `GroupId("some group")` but is now `"some group"`.
- ApiKey is now non_exhaustive.
- Added `gzip`, `zstd`, `snappy` and `lz4` features to enable the different compression algorithms for records (All enabled by default)

## v0.12.0

- Add `client` feature to enable encoding of requests and decoding of responses (enabled by default)
- Add `broker` feature to enable encoding of responses and decoding of requests (enabled by default)
- Add `messages_enums` feature to enable `ResponseKind` and `RequestKind` enums
- Add `encode`, `decode` and `header_version` methods to `ResponseKind` and `RequestKind`

## v0.11.0

- Replace `derive_builder` with custom impl.
- Remove `EncodeError`/`DecodeError` aliases
- Remove panic from `RecordBatchEncoder::encode`
- Use `IntoIterator` instead of `Iterator` for `RecordBatchEncoder::encode`
- Use CRC-32 ISO/HDLC instead of CRC-32 CKSUM.
- Add `Display` and more `From<T>` implementations for `StrBytes`.
- Avoid redundant variant names in RequestKind/ResponseKind.

## v0.10.2

- Implement `From<T>` for RequestKind and ResponseKind.

## v0.10.1

- Fix lz4 compression.

## v0.10.0

- Use `anyhow` for public error types. In general, these errors reflect non-recoverable programmer error and
should not occur when interacting with a well-formed Kafka implementation.
- Rename `StrBytes::from_str` to `from_static_str` to avoid overlap with the `FromStr` trait.
- Improve codegen, including filtering out unstable messages, improving ordering of structs in generated code,
and other misc improvements.
- Upgrade message version to upstream `v3.7.0` Kafka.

## v0.9.0

- Remove the `string` crate.
- Update `derive-builder`.

## v0.8.2

- Expose protocol::buf::gap module.

## v0.8.1

- Re-export `indexmap` as part of the public api.

## v0.8.0

- Implement Zstd and Lz4 compression.

## v0.7.0

- Switch to [crc32c](https://crates.io/crates/crc32c) crate, providing hardware acceleration for crc operations
on supported platforms.
- Formatting fixes.
- Miscellaneous dependency updates.
- Fix issue with `derive_builder` implementation in which default fields were not being used correctly.

## v0.6.1

- Miscellaneous dependency updates.

## v0.6.0

- Add `ApiKey::request_header_version` and `ApiKey::response_header_version` to assist deserializing
headers without reference to message type.

## v0.5.1

Bump `uuid` crate version.

## v0.5.0

- Clarify versioning support for upstream Kafka, tracking the latest Kafka
stable release. Improve docs.

#### Enhancements

- `Builder::builder` trait for retrieving builder instances from messages
without requiring extra builder imports.
- `#[non_exhaustive]` added to all items to ensure forward compatability
with protocol upgrades.

## v0.4.0

- Add utilities for dealing with response error codes.

## v0.3.0

#### Enhancements

- Build messages from crate root.
- Upgrade to latest version of Kafka protocol.

## v0.2.0

#### Enhancements

- Add derive builder for all messages.

## v0.1.0

Initial release.
