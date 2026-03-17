# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Derived from [tychedelia/kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs), which is no longer actively maintained. This is a Rust implementation of the Kafka wire protocol (`proto-kafka` crate, v0.17.0). Message types are **code-generated** from Kafka's JSON schema files, currently tracking Kafka 4.1.0. The crate covers all 87 API keys with full version support.

## Build Commands

```bash
# Build
cargo build --workspace --all-features

# Test (integration tests require Docker for testcontainers)
cargo test --workspace --all-features

# Run a single test
cargo test --all-features <test_name>

# Clippy (CI uses cargo-hack for feature powerset)
cargo hack --feature-powerset clippy --all-targets --locked -- -D warnings

# Rustfmt (only checked on protocol_codegen in CI)
cd protocol_codegen && cargo fmt -- --check

# Regenerate protocol messages from Kafka upstream
cargo run -p protocol_codegen

# Validate publishability
cargo publish --dry-run
```

## Architecture

### Generated vs Hand-Written Code

- `src/messages.rs` (~4200 lines) and `src/messages/` (185+ files) are **auto-generated** — do not edit by hand. Run `cargo run -p protocol_codegen` to regenerate.
- `protocol_codegen/` is the code generator workspace member. It clones the Kafka repo, parses JSON schemas from `clients/src/main/resources/common/message/`, and emits Rust structs with Encodable/Decodable impls.
- `protocol_codegen/src/generate_messages.rs` (~16K lines) contains the core generation logic.

### Core Modules (hand-written)

- **`src/protocol/`** — Core traits (`Message`, `Encodable`, `Decodable`, `Request`, `HeaderVersion`), primitive type encoders/decoders (`types.rs`), and buffer utilities with gap-based CRC computation (`buf.rs`).
- **`src/records.rs`** — Record batch encoding/decoding with compression, CRC-32c checksums, and `RecordIterator` for streaming across multiple batches.
- **`src/compression/`** — Pluggable compression (gzip, snappy, zstd, lz4). Snappy uses Kafka-compatible Java format with fallback decoding for non-Kafka snappy.
- **`src/error.rs`** — `ProtocolError` enum (thiserror-based), `Result<T>` type alias, `bail!` macro, `ResultExt` context trait, and Kafka `ResponseError` codes.

### Feature Flags

- `client` (default): Enables request encoding + response decoding.
- `broker` (default): Enables response encoding + request decoding.
- `messages_enums`: Adds `RequestKind`/`ResponseKind` enums. **Disabled by default** — doubles clean build time.
- `gzip`, `zstd`, `snappy`, `lz4` (all default): Compression algorithm support.

### Key Design Patterns

- All public types are `#[non_exhaustive]` — construct with `Default::default()` and builder methods (`with_*`).
- Every encode/decode call requires an explicit API version parameter.
- Messages contain **all** fields for every version (not `Option`); unused fields have default/nil values.
- `StrBytes` is a zero-copy string type backed by `bytes::Bytes`.
- Errors use `ProtocolError` (thiserror) via `crate::error::Result<T>`. No `anyhow` dependency.
- `#[allow(clippy::all)]` is applied to the generated `messages` module.

### Testing

- Integration tests in `tests/all_tests/` use `testcontainers` with a real Kafka broker (Docker required).
- `tests/all_tests/common.rs` provides test utilities: `start_kafka()`, `connect_to_kafka()`, `send_request()`/`receive_response()`.
- Rust toolchain: 1.88 (pinned in `rust-toolchain.toml`).
