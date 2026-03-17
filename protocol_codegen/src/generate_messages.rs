use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use failure::Error;

mod code_writer;
pub mod expr;
mod generate;
mod parse;
mod spec;

use spec::{SpecType, VersionSpec};

pub fn run(messages_module_dir: &str, mut input_file_paths: Vec<PathBuf>) -> Result<(), Error> {
    input_file_paths.sort();

    let module_path = format!("{}.rs", messages_module_dir);

    // generate file messages.rs
    let mut m = File::create(module_path)?;

    writeln!(m, "//! Messages used by the Kafka protocol.")?;
    writeln!(m, "//!")?;
    writeln!(m, "//! These messages are generated programmatically. See the [Kafka's protocol documentation](https://kafka.apache.org/protocol.html) for more information about a given message type.")?;
    writeln!(
        m,
        "// WARNING: the items of this module are generated and should not be edited directly."
    )?;
    writeln!(m)?;
    writeln!(
        m,
        "use crate::protocol::{{NewType, StrBytes, HeaderVersion}};"
    )?;
    writeln!(m, "#[cfg(all(feature = \"client\", feature = \"broker\"))]")?;
    writeln!(m, "use crate::protocol::Request;")?;
    writeln!(m, "use crate::protocol::VersionRange;")?;
    writeln!(m, "use std::convert::TryFrom;")?;
    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "#[cfg(any(feature = \"client\", feature = \"broker\"))]")?;
    writeln!(m, "use crate::protocol::Encodable;")?;
    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "#[cfg(any(feature = \"client\", feature = \"broker\"))]")?;
    writeln!(m, "use crate::protocol::Decodable;")?;
    writeln!(m, "use crate::error::Result;")?;
    writeln!(m)?;

    let mut entity_types = BTreeSet::new();
    let mut request_types = BTreeMap::new();
    let mut response_types = BTreeMap::new();
    let mut api_key_to_valid_version: HashMap<i16, VersionSpec> = HashMap::new();

    for input_file_path in &input_file_paths {
        let spec = parse::parse(input_file_path)?;
        let spec_meta = (spec.type_, spec.api_key);
        let valid_versions = spec.valid_versions;

        let outcome = generate::generate(messages_module_dir, spec)?;
        if let Some(output) = outcome {
            match spec_meta {
                (SpecType::Request, Some(k)) => {
                    request_types.insert(k, output);
                    api_key_to_valid_version.insert(k, valid_versions);
                }
                (SpecType::Response, Some(k)) => {
                    response_types.insert(k, output);
                }
                _ => {
                    output.apply(&mut m, &mut entity_types)?;
                }
            }
        }
    }

    {
        // require that each message must have both request and answer, and ignore
        // them if one side is missing
        let request_keys = request_types.keys().collect::<BTreeSet<_>>();
        let response_keys = response_types.keys().collect::<BTreeSet<_>>();
        let difference = request_keys
            .symmetric_difference(&response_keys)
            .map(|k| **k)
            .collect::<BTreeSet<_>>();

        for key in difference {
            request_types.remove(&key);
            response_types.remove(&key);
        }
    }

    for (_api_key, output) in request_types.iter().chain(response_types.iter()) {
        output.apply(&mut m, &mut entity_types)?;
    }

    // strip away the module name which is no longer needed
    let request_types = {
        let mut request_types = request_types
            .into_iter()
            .map(|(api_key, output)| (api_key, output.struct_name))
            .collect::<Vec<(_, _)>>();
        request_types.sort();
        request_types
    };

    let response_types = {
        let mut response_types = response_types
            .into_iter()
            .map(|(api_key, output)| (api_key, output.struct_name))
            .collect::<Vec<(_, _)>>();
        response_types.sort();
        response_types
    };

    for (api_key, request_type) in request_types.iter() {
        let response_type = response_types
            .iter()
            .find(|(k, _)| k == api_key)
            .map(|(_, v)| v)
            .expect("Every request type has a response type");
        writeln!(m, "#[cfg(all(feature = \"client\", feature = \"broker\"))]")?;
        writeln!(m, "impl Request for {} {{", request_type)?;
        writeln!(m, "    const KEY: i16 = {};", api_key)?;
        writeln!(m, "    type Response = {};", response_type)?;
        writeln!(m, "}}")?;
        writeln!(m)?;
    }

    writeln!(m, "/// Valid API keys in the Kafka protocol.")?;
    writeln!(m, "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]")?;
    writeln!(m, "pub enum ApiKey {{")?;
    for (api_key, request_type) in request_types.iter() {
        writeln!(m, "    /// API key for request {}", request_type)?;
        writeln!(
            m,
            "    {} = {},",
            request_type.replace("Request", ""),
            api_key
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m)?;

    writeln!(m, "impl ApiKey {{")?;
    writeln!(
        m,
        "    /// Get the version of request header that needs to be prepended to this message"
    )?;
    writeln!(
        m,
        "    pub fn request_header_version(&self, version: i16) -> i16 {{"
    )?;
    writeln!(m, "        match self {{")?;
    for (_api_key, request_type) in request_types.iter() {
        writeln!(
            m,
            "            ApiKey::{} => {}::header_version(version),",
            request_type.replace("Request", ""),
            request_type
        )?;
    }
    writeln!(m, "        }}")?;
    writeln!(m, "    }}")?;

    writeln!(
        m,
        "    /// Get the version of response header that needs to be prepended to this message"
    )?;
    writeln!(
        m,
        "    pub fn response_header_version(&self, version: i16) -> i16 {{"
    )?;
    writeln!(m, "        match self {{")?;
    for (_api_key, response_type) in response_types.iter() {
        writeln!(
            m,
            "            ApiKey::{} => {}::header_version(version),",
            response_type.replace("Response", ""),
            response_type
        )?;
    }
    writeln!(m, "        }}")?;
    writeln!(m, "    }}")?;

    writeln!(
        m,
        "    /// Returns the valid versions that can be used with this ApiKey"
    )?;
    writeln!(m, "    pub fn valid_versions(&self) -> VersionRange {{")?;
    writeln!(m, "        match self {{")?;
    for (api_key, request_type) in request_types.iter() {
        let valid_versions = api_key_to_valid_version
            .get(api_key)
            .unwrap()
            .range()
            .unwrap();
        writeln!(
            m,
            "ApiKey::{} => VersionRange {{ min: {}, max: {} }},",
            request_type.replace("Request", ""),
            valid_versions.start(),
            valid_versions.end(),
        )?;
    }
    writeln!(m, "        }}")?;
    writeln!(m, "    }}")?;

    writeln!(
        m,
        r#"
  /// Iterate through every ApiKey variant in the order of the internal code.
  pub fn iter() -> impl Iterator<Item = ApiKey> {{
    (0..={}).filter_map(|i| ApiKey::try_from(i).ok())
  }}
    "#,
        request_types.iter().map(|x| x.0).max().unwrap()
    )?;

    writeln!(m, "}}")?;

    writeln!(m, "impl TryFrom<i16> for ApiKey {{")?;
    writeln!(m, "    type Error = ();")?;
    writeln!(m)?;
    writeln!(
        m,
        "    fn try_from(v: i16) -> std::result::Result<Self, Self::Error> {{"
    )?;
    writeln!(m, "        match v {{")?;
    for (_, request_type) in request_types.iter() {
        let key = request_type.replace("Request", "");
        writeln!(
            m,
            "            x if x == ApiKey::{} as i16 => Ok(ApiKey::{}),",
            key, key
        )?;
    }
    writeln!(m, "            _ => Err(()),")?;
    writeln!(m, "        }}")?;
    writeln!(m, "    }}")?;
    writeln!(m, "}}")?;
    writeln!(m)?;

    writeln!(
        m,
        "/// Wrapping enum for all requests in the Kafka protocol."
    )?;
    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "#[non_exhaustive]")?;
    writeln!(m, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(m, "pub enum RequestKind {{")?;
    for (_, request_type) in request_types.iter() {
        writeln!(m, "    /// {},", request_type)?;
        writeln!(
            m,
            "    {}({}),",
            request_type.trim_end_matches("Request"),
            request_type
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m)?;

    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "impl RequestKind {{")?;
    writeln!(m, "/// Encode the message into the target buffer")?;
    writeln!(m, "#[cfg(feature = \"client\")]")?;
    writeln!(
        m,
        "pub fn encode(&self, bytes: &mut bytes::BytesMut, version: i16) -> crate::error::Result<()> {{"
    )?;
    writeln!(m, "match self {{")?;
    for (_, request_type) in request_types.iter() {
        let variant = request_type.trim_end_matches("Request");
        writeln!(m, "RequestKind::{variant}(x) => encode(x, bytes, version),")?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;

    writeln!(
        m,
        "/// Decode the message from the provided buffer and version"
    )?;
    writeln!(m, "#[cfg(feature = \"broker\")]")?;
    writeln!(
        m,
        "pub fn decode(api_key: ApiKey, bytes: &mut bytes::Bytes, version: i16) -> crate::error::Result<RequestKind> {{"
    )?;
    writeln!(m, "match api_key {{")?;
    for (_, request_type) in request_types.iter() {
        let variant = request_type.trim_end_matches("Request");
        writeln!(
            m,
            "ApiKey::{variant} => Ok(RequestKind::{variant}(decode(bytes, version)?)),"
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;

    writeln!(m, "}}")?;

    for (_, request_type) in request_types.iter() {
        writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
        writeln!(m, "impl From<{request_type}> for RequestKind {{")?;
        writeln!(m, "    fn from(value: {request_type}) -> RequestKind {{")?;
        let variant = request_type.trim_end_matches("Request");
        writeln!(m, "        RequestKind::{variant}(value)")?;
        writeln!(m, "    }}")?;
        writeln!(m, "}}")?;
        writeln!(m)?;
    }

    writeln!(
        m,
        r#"
#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
fn decode<T: Decodable>(bytes: &mut bytes::Bytes, version: i16) -> Result<T> {{
    T::decode(bytes, version).map_err(|e| {{
        crate::error::ProtocolError::Other(format!(
            "Failed to decode {{}} v{{}} body: {{e}}",
            std::any::type_name::<T>(),
            version
        ))
    }})
}}

#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
fn encode<T: Encodable>(encodable: &T, bytes: &mut bytes::BytesMut, version: i16) -> Result<()> {{
    encodable.encode(bytes, version).map_err(|e| {{
        crate::error::ProtocolError::Other(format!(
            "Failed to encode {{}} v{{}} body: {{e}}",
            std::any::type_name::<T>(),
            version
        ))
    }})
}}

#[cfg(feature = "messages_enums")]
#[cfg(feature = "broker")]
fn encode_into<T: Encodable, B: crate::protocol::buf::ByteBufMut>(encodable: &T, buf: &mut B, version: i16) -> Result<()> {{
    encodable.encode(buf, version).map_err(|e| {{
        crate::error::ProtocolError::Other(format!(
            "Failed to encode {{}} v{{}} body: {{e}}",
            std::any::type_name::<T>(),
            version
        ))
    }})
}}
    "#
    )?;

    writeln!(
        m,
        "/// Wrapping enum for all responses in the Kafka protocol."
    )?;
    writeln!(m, "#[non_exhaustive]")?;
    writeln!(m, "#[derive(Debug, Clone, PartialEq)]")?;
    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "pub enum ResponseKind {{")?;
    for (_, response_type) in response_types.iter() {
        writeln!(m, "    /// {},", response_type)?;
        writeln!(
            m,
            "    {}({}),",
            response_type.trim_end_matches("Response"),
            response_type
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m)?;

    writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
    writeln!(m, "impl ResponseKind {{")?;
    writeln!(m, "/// Encode the message into the target buffer")?;
    writeln!(m, "#[cfg(feature = \"broker\")]")?;
    writeln!(
        m,
        "pub fn encode(&self, bytes: &mut bytes::BytesMut, version: i16) -> crate::error::Result<()> {{"
    )?;
    writeln!(m, "match self {{")?;
    for (_, response_type) in response_types.iter() {
        let variant = response_type.trim_end_matches("Response");
        writeln!(
            m,
            "ResponseKind::{variant}(x) => encode(x, bytes, version),"
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;

    writeln!(
        m,
        "/// Encode the message into a `ByteBufMut` (e.g. `SegmentedBuf` for zero-copy)."
    )?;
    writeln!(m, "#[cfg(feature = \"broker\")]")?;
    writeln!(
        m,
        "pub fn encode_into<B: crate::protocol::buf::ByteBufMut>(&self, buf: &mut B, version: i16) -> crate::error::Result<()> {{"
    )?;
    writeln!(m, "match self {{")?;
    for (_, response_type) in response_types.iter() {
        let variant = response_type.trim_end_matches("Response");
        writeln!(
            m,
            "ResponseKind::{variant}(x) => encode_into(x, buf, version),"
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;

    writeln!(
        m,
        "/// Decode the message from the provided buffer and version"
    )?;
    writeln!(m, "#[cfg(feature = \"client\")]")?;
    writeln!(
        m,
        "pub fn decode(api_key: ApiKey, bytes: &mut bytes::Bytes, version: i16) -> crate::error::Result<ResponseKind> {{"
    )?;
    writeln!(m, "match api_key {{")?;
    for (_, response_type) in response_types.iter() {
        let variant = response_type.trim_end_matches("Response");
        writeln!(
            m,
            "ApiKey::{variant} => Ok(ResponseKind::{variant}(decode(bytes, version)?)),"
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;

    writeln!(
        m,
        "/// Get the version of request header that needs to be prepended to this message"
    )?;
    writeln!(m, "pub fn header_version(&self, version: i16) -> i16 {{")?;
    writeln!(m, "match self {{")?;
    for (_, response_type) in response_types.iter() {
        let variant = response_type.trim_end_matches("Response");
        writeln!(
            m,
            "ResponseKind::{variant}(_) => {response_type}::header_version(version),"
        )?;
    }
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;
    writeln!(m, "}}")?;
    writeln!(m)?;

    for (_, response_type) in response_types.iter() {
        writeln!(m, "#[cfg(feature = \"messages_enums\")]")?;
        writeln!(m, "impl From<{response_type}> for ResponseKind {{")?;
        writeln!(m, "    fn from(value: {response_type}) -> ResponseKind {{")?;
        let variant = response_type.trim_end_matches("Response");
        writeln!(m, "        ResponseKind::{variant}(value)")?;
        writeln!(m, "    }}")?;
        writeln!(m, "}}")?;
        writeln!(m)?;
    }

    for entity_type in entity_types {
        let mut derives = vec![
            "Clone",
            "Eq",
            "PartialEq",
            "Ord",
            "PartialOrd",
            "Hash",
            "Default",
        ];
        if entity_type.inner.is_copy() {
            derives.push("Copy");
        }

        let rust_name = entity_type.inner.rust_name();

        writeln!(m, "/// {}", entity_type.doc)?;
        writeln!(m, "#[derive({})]", derives.join(", "))?;
        writeln!(m, "pub struct {}(pub {});\n", entity_type.name, rust_name)?;
        writeln!(m, "impl From<{}> for {} {{", rust_name, entity_type.name)?;
        writeln!(
            m,
            "    fn from(other: {}) -> Self {{ Self(other) }}",
            rust_name
        )?;
        writeln!(m, "}}")?;
        writeln!(m, "impl From<{}> for {} {{", entity_type.name, rust_name)?;
        writeln!(
            m,
            "    fn from(other: {}) -> Self {{ other.0 }}",
            entity_type.name
        )?;
        writeln!(m, "}}")?;
        writeln!(
            m,
            "impl std::borrow::Borrow<{}> for {} {{",
            rust_name, entity_type.name
        )?;
        writeln!(m, "    fn borrow(&self) -> &{} {{ &self.0 }}", rust_name)?;
        writeln!(m, "}}")?;
        writeln!(m, "impl std::ops::Deref for {} {{", entity_type.name)?;
        writeln!(m, "    type Target = {};", rust_name)?;
        writeln!(m, "    fn deref(&self) -> &Self::Target {{ &self.0 }}")?;
        writeln!(m, "}}")?;
        writeln!(
            m,
            "impl std::cmp::PartialEq<{}> for {} {{",
            rust_name, entity_type.name
        )?;
        writeln!(
            m,
            "    fn eq(&self, other: &{}) -> bool {{ &self.0 == other }}",
            rust_name
        )?;
        writeln!(m, "}}")?;
        writeln!(
            m,
            "impl std::cmp::PartialEq<{}> for {} {{",
            entity_type.name, rust_name
        )?;
        writeln!(
            m,
            "    fn eq(&self, other: &{}) -> bool {{ self == &other.0 }}",
            entity_type.name
        )?;
        writeln!(m, "}}")?;

        writeln!(m, "impl std::fmt::Debug for {} {{", entity_type.name)?;
        writeln!(
            m,
            "    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{ self.0.fmt(f) }}",
        )?;
        writeln!(m, "}}")?;

        writeln!(
            m,
            "impl NewType<{}> for {} {{}}",
            rust_name, entity_type.name
        )?;
        writeln!(m)?;
    }

    Ok(())
}
