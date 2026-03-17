//! Post-generation customizations for specific message types.
//!
//! These customizations add fields and methods to generated structs that are not
//! part of the upstream Kafka protocol schema but are needed by downstream consumers
//! (e.g., zero-copy multi-batch fetch responses).
//!
//! After running the code generator, `apply()` patches the generated files in-place.
//! This ensures customizations survive regeneration.

use std::path::Path;

/// Apply all post-generation customizations to the generated message files.
pub fn apply(messages_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    patch_fetch_response_partition_data(messages_dir)?;
    Ok(())
}

/// Replace `old` with `new` in `content` exactly once, panicking with a descriptive message if
/// the anchor is not found.
fn replace_once(content: &str, old: &str, new: &str, description: &str) -> String {
    assert!(
        content.contains(old),
        "Customization anchor not found: {description}\nExpected:\n{old}"
    );
    content.replacen(old, new, 1)
}

/// Add `record_segments` field, `for_fetch` constructor, and zero-copy encode/decode
/// to `PartitionData` in `fetch_response.rs`.
fn patch_fetch_response_partition_data(
    messages_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(messages_dir).join("fetch_response.rs");
    let mut content = std::fs::read_to_string(&path)?;

    // 1. Add `record_segments` field to PartitionData struct.
    //    Anchor: the records field followed by unknown_tagged_fields, uniquely in PartitionData.
    content = replace_once(
        &content,
        "pub records: Option<Bytes>,\n\n    /// Other tagged fields\n    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,\n}\n\nimpl PartitionData {\n    /// Sets `partition_index`",
        "pub records: Option<Bytes>,\n\n    /// Pre-encoded record batch segments for zero-copy multi-batch encoding.\n    ///\n    /// When non-empty, these segments are written directly instead of `records`.\n    /// This avoids copying record batches into a single contiguous buffer.\n    pub record_segments: Vec<Bytes>,\n\n    /// Other tagged fields\n    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,\n}\n\nimpl PartitionData {\n    /// Sets `partition_index`",
        "record_segments field in PartitionData",
    );

    // 2. Add `with_record_segments` builder and `for_fetch` constructor.
    //    Anchor: closing of impl PartitionData block right before the broker Encodable impl.
    content = replace_once(
        &content,
        "}\n\n#[cfg(feature = \"broker\")]\nimpl Encodable for PartitionData {",
        concat!(
            "    /// Sets `record_segments` for zero-copy multi-batch encoding.\n",
            "    pub fn with_record_segments(mut self, value: Vec<Bytes>) -> Self {\n",
            "        self.record_segments = value;\n",
            "        self\n",
            "    }\n",
            "    /// Constructs a partition response with only the fields needed for fetch.\n",
            "    /// Avoids the allocations in `Default` (BTreeMap, Vec, Bytes).\n",
            "    #[inline]\n",
            "    pub fn for_fetch(\n",
            "        partition_index: i32,\n",
            "        high_watermark: i64,\n",
            "        records: Option<Bytes>,\n",
            "        record_segments: Vec<Bytes>,\n",
            "        error_code: i16,\n",
            "        log_start_offset: i64,\n",
            "    ) -> Self {\n",
            "        Self {\n",
            "            partition_index,\n",
            "            error_code,\n",
            "            high_watermark,\n",
            "            last_stable_offset: high_watermark,\n",
            "            log_start_offset,\n",
            "            diverging_epoch: Default::default(),\n",
            "            current_leader: Default::default(),\n",
            "            snapshot_id: Default::default(),\n",
            "            aborted_transactions: None,\n",
            "            preferred_read_replica: (-1).into(),\n",
            "            records,\n",
            "            record_segments,\n",
            "            unknown_tagged_fields: BTreeMap::new(),\n",
            "        }\n",
            "    }\n",
            "}\n\n",
            "#[cfg(feature = \"broker\")]\nimpl Encodable for PartitionData {",
        ),
        "with_record_segments and for_fetch methods",
    );

    // 3. Patch encode: replace records encoding with record_segments-aware version.
    //    Use a unique anchor that only appears in PartitionData's Encodable impl.
    content = replace_once(
        &content,
        "if version >= 12 {\n            types::CompactBytes.encode(buf, &self.records)?;\n        } else {\n            types::Bytes.encode(buf, &self.records)?;\n        }\n        if version >= 12 {\n            let mut num_tagged_fields = self.unknown_tagged_fields.len();\n            if &self.diverging_epoch != &Default::default()",
        concat!(
            "if !self.record_segments.is_empty() {\n",
            "            // Multi-batch zero-copy: write total length prefix, then each segment\n",
            "            let total_len: usize = self.record_segments.iter().map(|s| s.len()).sum();\n",
            "            if version >= 12 {\n",
            "                types::UnsignedVarInt.encode(buf, (total_len as u32) + 1)?;\n",
            "            } else {\n",
            "                if total_len > i32::MAX as usize {\n",
            "                    bail!(\"Record segments too large to encode ({} bytes)\", total_len);\n",
            "                }\n",
            "                types::Int32.encode(buf, total_len as i32)?;\n",
            "            }\n",
            "            for segment in &self.record_segments {\n",
            "                buf.put_shared_bytes(segment.clone());\n",
            "            }\n",
            "        } else if version >= 12 {\n",
            "            types::CompactBytes.encode(buf, &self.records)?;\n",
            "        } else {\n",
            "            types::Bytes.encode(buf, &self.records)?;\n",
            "        }\n",
            "        if version >= 12 {\n",
            "            let mut num_tagged_fields = self.unknown_tagged_fields.len();\n",
            "            if &self.diverging_epoch != &Default::default()",
        ),
        "record_segments in PartitionData encode",
    );

    // 4. Patch compute_size: replace records size computation.
    content = replace_once(
        &content,
        "if version >= 12 {\n            total_size += types::CompactBytes.compute_size(&self.records)?;\n        } else {\n            total_size += types::Bytes.compute_size(&self.records)?;\n        }\n        if version >= 12 {\n            let mut num_tagged_fields = self.unknown_tagged_fields.len();\n            if &self.diverging_epoch != &Default::default()",
        concat!(
            "if !self.record_segments.is_empty() {\n",
            "            let total_len: usize = self.record_segments.iter().map(|s| s.len()).sum();\n",
            "            if version >= 12 {\n",
            "                total_size += types::UnsignedVarInt.compute_size((total_len as u32) + 1)?;\n",
            "            } else {\n",
            "                total_size += 4; // i32 length prefix\n",
            "            }\n",
            "            total_size += total_len;\n",
            "        } else if version >= 12 {\n",
            "            total_size += types::CompactBytes.compute_size(&self.records)?;\n",
            "        } else {\n",
            "            total_size += types::Bytes.compute_size(&self.records)?;\n",
            "        }\n",
            "        if version >= 12 {\n",
            "            let mut num_tagged_fields = self.unknown_tagged_fields.len();\n",
            "            if &self.diverging_epoch != &Default::default()",
        ),
        "record_segments in PartitionData compute_size",
    );

    // 5. Patch decode: add record_segments to Ok(Self { ... }).
    //    Use a very specific anchor: the records field followed by unknown_tagged_fields in Ok(Self.
    content = replace_once(
        &content,
        "records,\n            unknown_tagged_fields,\n        })\n    }\n}\n\nimpl Default for PartitionData {",
        "records,\n            record_segments: Vec::new(),\n            unknown_tagged_fields,\n        })\n    }\n}\n\nimpl Default for PartitionData {",
        "record_segments in PartitionData decode",
    );

    // 6. Patch Default: add record_segments.
    content = replace_once(
        &content,
        "records: Some(Default::default()),\n            unknown_tagged_fields: BTreeMap::new(),\n        }\n    }\n}\n",
        "records: Some(Default::default()),\n            record_segments: Vec::new(),\n            unknown_tagged_fields: BTreeMap::new(),\n        }\n    }\n}\n",
        "record_segments in PartitionData Default",
    );

    std::fs::write(&path, content)?;
    println!("Applied customizations to fetch_response.rs");
    Ok(())
}
