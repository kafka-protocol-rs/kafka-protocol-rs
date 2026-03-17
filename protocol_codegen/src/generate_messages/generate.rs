use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};

use failure::Error;
use inflector::Inflector;

use super::code_writer::CodeWriter;
use super::expr::{CmpType, Expr};
use super::spec::{FieldSpec, PrimitiveType, Spec, SpecType, TypeSpec, VersionSpec};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedStruct {
    spec_type: SpecType,
    name: String,
    prepared_fields: Vec<PreparedField>,
    valid_versions: VersionSpec,
    flexible_msg_versions: VersionSpec,
    deprecated_versions: VersionSpec,
}

#[derive(Debug, Clone)]
pub struct EntityType {
    pub name: String,
    pub doc: String,
    pub inner: PrimitiveType,
}

impl PartialOrd for EntityType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.name.cmp(&other.name))
    }
}

impl Ord for EntityType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialEq for EntityType {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for EntityType {}

fn primitive_default(prim: PrimitiveType) -> PreparedDefault {
    use PrimitiveType::*;
    match prim {
        Bool => PreparedDefault::Boolean(false),
        Int8 | Int16 | Uint16 | Int32 | Int64 => PreparedDefault::Numeric("0".into()),
        Float64 => PreparedDefault::Numeric("0.0".into()),
        String | Bytes | Records => PreparedDefault::Empty,
        Uuid => PreparedDefault::Uuid,
    }
}

fn parse_primitive_default(
    prim: PrimitiveType,
    default_str: String,
    type_: &PreparedType,
) -> PreparedDefault {
    use PrimitiveType::*;
    match prim {
        Int8 | Int16 | Int32 | Int64 | Float64 => PreparedDefault::Numeric(default_str),
        String => PreparedDefault::String(default_str),
        _ => panic!(
            "Unexpected default value {:?} for {}",
            default_str,
            type_.rust_name()
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PreparedType {
    Primitive(PrimitiveType),
    Entity(EntityType),
    Struct(PreparedStruct),
    Array(Box<PreparedType>),
}

impl PreparedType {
    fn rust_name(&self) -> String {
        match self {
            Self::Primitive(prim) => prim.rust_name().into(),
            Self::Entity(entity_type) => format!("super::{}", entity_type.name),
            Self::Struct(inner) => inner.name.clone(),
            Self::Array(inner) => format!("Vec<{}>", inner.rust_name()),
        }
    }
    fn name(&self, flexible: bool, optional: bool) -> String {
        match self {
            Self::Primitive(prim) => prim.name(flexible).into(),
            Self::Entity(entity_type) => entity_type.inner.name(flexible).into(),
            Self::Struct(_) => {
                if optional {
                    "types::OptionStruct { version }".into()
                } else {
                    "types::Struct { version }".into()
                }
            }
            Self::Array(inner) => {
                if flexible {
                    format!("types::CompactArray({})", inner.name(flexible, false))
                } else {
                    format!("types::Array({})", inner.name(flexible, false))
                }
            }
        }
    }
    pub fn has_compact_form(&self) -> bool {
        match self {
            Self::Primitive(prim) => prim.has_compact_form(),
            Self::Entity(entity_type) => entity_type.inner.has_compact_form(),
            Self::Struct(_) => false,
            Self::Array(_) => true,
        }
    }
    fn default(&self) -> PreparedDefault {
        match self {
            Self::Primitive(prim) => primitive_default(*prim),
            Self::Entity(entity_type) => primitive_default(entity_type.inner),
            Self::Struct(_) => PreparedDefault::EmptyStruct,
            Self::Array(_) => PreparedDefault::Empty,
        }
    }
    fn is_entity(&self) -> bool {
        matches!(self, Self::Entity(_))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum PreparedDefault {
    Null,
    Empty,
    Boolean(bool),
    Numeric(String),
    String(String),
    Uuid,
    EmptyStruct,
}

impl PreparedDefault {
    fn gen_is_default(&self, expr: &Expr, optional: bool) -> Expr {
        if optional {
            if let Self::Null = self {
                expr.method("is_none", "")
            } else {
                expr.method("as_ref", "")
                    .method(
                        "map",
                        format!("|x| {}", self.gen_is_default(&Expr::new_atom("x"), false)),
                    )
                    .method("unwrap_or_default", "")
            }
        } else {
            match self {
                Self::Null => unreachable!(),
                Self::Empty => expr.method("is_empty", ""),
                Self::Boolean(true) => expr.deref(),
                Self::Boolean(false) => expr.deref().not(),
                Self::Numeric(v) => expr.deref().compare(CmpType::Eq, &Expr::new_atom(v)),
                Self::String(s) => expr.compare(CmpType::Eq, &Expr::new_str(s)),
                Self::Uuid => expr.compare(CmpType::Eq, &Expr::new_atom("&Uuid::nil()")),
                Self::EmptyStruct => {
                    expr.compare(CmpType::Eq, &Expr::new_atom("&Default::default()"))
                }
            }
        }
    }
    fn gen_default(&self, optional: bool, is_entity: bool) -> Expr {
        if optional {
            if let Self::Null = self {
                Expr::new_atom("None")
            } else {
                Expr::new_atom(&format!("Some({})", self.gen_default(false, is_entity)))
            }
        } else if is_entity {
            if let Self::Empty = self {
                self.gen_default(false, false)
            } else {
                self.gen_default(false, false).method("into", "")
            }
        } else {
            match self {
                Self::Null => unreachable!(),
                // TODO: unclear if empty struct default value can be used as zero val for comparison
                Self::Empty | Self::EmptyStruct => Expr::new_atom("Default::default()"),
                Self::Boolean(true) => Expr::new_atom("true"),
                Self::Boolean(false) => Expr::new_atom("false"),
                Self::Numeric(v) => Expr::new_unary(v),
                Self::String(s) => Expr::new_atom(&format!("StrBytes::from_static_str({:?})", s)),
                Self::Uuid => Expr::new_atom("Uuid::nil()"),
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PreparedField {
    name: String,
    optional: bool,
    type_: PreparedType,
    versions: VersionSpec,
    tag: Option<i32>,
    tagged_versions: VersionSpec,
    nullable_versions: VersionSpec,
    default: PreparedDefault,
    ignorable: bool,
    _entity_type: Option<String>,
    about: String,
    flexible_versions: VersionSpec,
}

impl PreparedField {
    fn var_name(&self) -> Expr {
        Expr::new_atom("self").field(&self.name).by_ref()
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_field_type<W: Write>(
    w: &mut CodeWriter<W>,
    type_: &TypeSpec,
    field: &FieldSpec,
    entity_types: &mut BTreeSet<EntityType>,
    valid_versions: VersionSpec,
    flexible_msg_versions: VersionSpec,
    deprecated_versions: VersionSpec,
    prepared_structs: &BTreeMap<String, PreparedStruct>,
    prepared_structs_output: &mut Vec<PreparedStruct>,
    spec_type: SpecType,
) -> Result<PreparedType, Error> {
    Ok(match type_ {
        TypeSpec::Primitive(prim) => {
            if let Some(entity_type) = &field.entity_type {
                let entity_type = EntityType {
                    inner: *prim,
                    name: entity_type.to_pascal_case(),
                    doc: field.about.clone(),
                };
                entity_types.insert(entity_type.clone());
                PreparedType::Entity(entity_type)
            } else {
                PreparedType::Primitive(*prim)
            }
        }
        TypeSpec::Struct(name) => {
            let prepared_struct = if field.fields.is_empty() {
                // The struct could be better defined in the written_structs list.
                // In particular, it could contain a map key.
                if let Some(found) = prepared_structs.get(name) {
                    found.clone()
                } else {
                    PreparedStruct {
                        name: name.clone(),
                        prepared_fields: vec![],
                        valid_versions,
                        flexible_msg_versions,
                        deprecated_versions,
                        spec_type,
                    }
                }
            } else {
                prepared_struct_def(
                    w,
                    name,
                    &field.fields,
                    entity_types,
                    valid_versions,
                    flexible_msg_versions,
                    deprecated_versions,
                    prepared_structs,
                    prepared_structs_output,
                    spec_type,
                )?
            };

            PreparedType::Struct(prepared_struct)
        }
        TypeSpec::Array(elem) => {
            let prepared_field = prepare_field_type(
                w,
                elem,
                field,
                entity_types,
                valid_versions,
                flexible_msg_versions,
                deprecated_versions,
                prepared_structs,
                prepared_structs_output,
                spec_type,
            )?;
            PreparedType::Array(Box::new(prepared_field))
        }
    })
}

fn write_version_cond<
    W: Write,
    FT: FnOnce(&mut CodeWriter<W>) -> Result<(), Error>,
    FF: FnOnce(&mut CodeWriter<W>) -> Result<(), Error>,
>(
    w: &mut CodeWriter<W>,
    valid_versions: VersionSpec,
    condition: VersionSpec,
    if_true: FT,
    if_false: FF,
    skip_true: bool,
    skip_false: bool,
) -> Result<(), Error> {
    let (min, max) = match valid_versions {
        VersionSpec::None => return Ok(()),
        VersionSpec::Exact(version) => (version, version),
        VersionSpec::Since(_) => panic!("Valid version range should be bounded"),
        VersionSpec::Range(a, b) => (a, b),
    };

    let condition = condition.intersect(valid_versions);
    let always_true = condition == valid_versions;
    if skip_true {
        if !always_true {
            match condition {
                VersionSpec::None => return if_false(w),
                VersionSpec::Exact(version) if version == max => {
                    write!(w, "if version < {} ", version)?
                }
                VersionSpec::Exact(version) => write!(w, "if version != {} ", version)?,
                VersionSpec::Since(version) => write!(w, "if version < {} ", version)?,
                VersionSpec::Range(a, b) if a == min => write!(w, "if version > {} ", b)?,
                VersionSpec::Range(a, b) if b == max => write!(w, "if version < {} ", a)?,
                VersionSpec::Range(a, b) => write!(w, "if version < {} || version > {} ", a, b)?,
            }
            w.block(if_false)?;
        }
    } else if always_true {
        if_true(w)?;
    } else {
        match condition {
            VersionSpec::None => return if_false(w),
            VersionSpec::Exact(version) if version == max => {
                write!(w, "if version >= {} ", version)?
            }
            VersionSpec::Exact(version) => write!(w, "if version == {} ", version)?,
            VersionSpec::Since(version) => write!(w, "if version >= {} ", version)?,
            VersionSpec::Range(a, b) if a == min => write!(w, "if version <= {} ", b)?,
            VersionSpec::Range(a, b) if b == max => write!(w, "if version >= {} ", a)?,
            VersionSpec::Range(a, b) => write!(w, "if version >= {} && version <= {} ", a, b)?,
        }
        w.block(if_true)?;
        if !skip_false {
            write!(w, " else ")?;
            w.block(if_false)?;
        }
    }
    Ok(())
}

fn write_encode_or_compute<W: Write, T: Display>(
    w: &mut CodeWriter<W>,
    type_: &str,
    var_name: T,
    compute_size: bool,
) -> Result<(), Error> {
    if compute_size {
        write!(w, "total_size += {}.compute_size({})?;", type_, var_name)?;
    } else {
        write!(w, "{}.encode(buf, {})?;", type_, var_name)?;
    }
    Ok(())
}

fn write_encode_field<W: Write>(
    w: &mut CodeWriter<W>,
    field: &PreparedField,
    valid_versions: VersionSpec,
    compute_size: bool,
) -> Result<(), Error> {
    if field
        .tagged_versions
        .contains(field.versions.intersect(valid_versions))
    {
        return Ok(());
    }

    write_version_cond(
        w,
        valid_versions,
        field.tagged_versions,
        |_| Ok(()),
        |w| {
            write_version_cond(
                w,
                valid_versions,
                field.versions,
                // field is used in this version, encode it
                |w| write_encode_field_inner(w, field, valid_versions, compute_size),
                // field is not present in this version, ensure that the default value is used
                |w| write_fail_if_field_set(w, field),
                false,
                field.ignorable,
            )
        },
        true,
        false,
    )?;
    writeln!(w)?;
    Ok(())
}

fn write_encode_field_inner<W: Write>(
    w: &mut CodeWriter<W>,
    field: &PreparedField,
    valid_versions: VersionSpec,
    compute_size: bool,
) -> Result<(), Error> {
    let var_name = field.var_name();
    let optional = !field.nullable_versions.is_none();

    let valid_versions = valid_versions.intersect(field.versions);
    if !field.type_.has_compact_form() {
        write_encode_or_compute(
            w,
            &field.type_.name(false, optional),
            &var_name,
            compute_size,
        )
    } else {
        write_version_cond(
            w,
            valid_versions,
            field.flexible_versions,
            |w| {
                write_encode_or_compute(
                    w,
                    &field.type_.name(true, optional),
                    &var_name,
                    compute_size,
                )
            },
            |w| {
                write_encode_or_compute(
                    w,
                    &field.type_.name(false, optional),
                    &var_name,
                    compute_size,
                )
            },
            false,
            false,
        )
    }
}

fn write_fail_if_field_set<W: Write>(
    w: &mut CodeWriter<W>,
    field: &PreparedField,
) -> Result<(), Error> {
    let var_name = field.var_name();

    let is_not_default = field
        .default
        .gen_is_default(&var_name, field.optional)
        .not();
    write!(w, "if {is_not_default} ")?;
    w.block(|w| {
        write!(
            w,
            r#"bail!("A field is set that is not available on the selected protocol version");"#
        )?;
        Ok(())
    })
}

fn write_size_check<W: Write, T: Display>(
    w: &mut CodeWriter<W>,
    expr: T,
    t: &str,
    msg: &str,
) -> Result<(), Error> {
    writeln!(w, "if {} > std::{}::MAX as usize {{", expr, t)?;
    writeln!(w, "    bail!({:?}, {});", msg, expr)?;
    writeln!(w, "}}")?;
    Ok(())
}

fn write_encode_tag_buffer<W: Write>(
    w: &mut CodeWriter<W>,
    prepared_fields: &[PreparedField],
    valid_versions: VersionSpec,
    flexible_msg_versions: VersionSpec,
    compute_size: bool,
) -> Result<(), Error> {
    write_version_cond(
        w,
        valid_versions,
        flexible_msg_versions,
        |w| {
            let valid_versions = valid_versions.intersect(flexible_msg_versions);

            let sorted_tagged_fields: BTreeMap<i32, &PreparedField> = prepared_fields
                .iter()
                .filter_map(|field| Some((field.tag?, field)))
                .collect();

            if sorted_tagged_fields.is_empty() {
                writeln!(
                    w,
                    "let num_tagged_fields = self.unknown_tagged_fields.len();"
                )?;
            } else {
                writeln!(
                    w,
                    "let mut num_tagged_fields = self.unknown_tagged_fields.len();"
                )?;
            }

            // Count number of tagged fields
            for field in sorted_tagged_fields.values() {
                let var_name = &field.var_name();
                write_version_cond(
                    w,
                    valid_versions,
                    field.tagged_versions,
                    |w| {
                        let valid_versions = valid_versions.intersect(field.tagged_versions);
                        write_version_cond(
                            w,
                            valid_versions,
                            field.versions,
                            |w| {
                                let is_default =
                                    field.default.gen_is_default(var_name, field.optional).not();
                                write!(w, "if {is_default} ")?;
                                w.block(|w| {
                                    write!(w, "num_tagged_fields += 1;")?;
                                    Ok(())
                                })
                            },
                            |_| Ok(()),
                            false,
                            true,
                        )?;
                        writeln!(w)?;
                        Ok(())
                    },
                    |_| Ok(()),
                    false,
                    true,
                )?;
            }

            write_size_check(
                w,
                "num_tagged_fields",
                "u32",
                "Too many tagged fields to encode ({} fields)",
            )?;
            write_encode_or_compute(
                w,
                "types::UnsignedVarInt",
                "num_tagged_fields as u32",
                compute_size,
            )?;
            writeln!(w)?;

            // Write out tagged fields
            let mut current_tag = -1;
            for (&k, field) in &sorted_tagged_fields {
                current_tag += 1;
                if k != current_tag {
                    if !compute_size {
                        writeln!(w, "write_unknown_tagged_fields(buf, {}..{}, &self.unknown_tagged_fields)?;", current_tag, k)?;
                    }
                    current_tag = k;
                }

                write_version_cond(
                    w,
                    valid_versions,
                    field.tagged_versions,
                    |w| {
                        let valid_versions = valid_versions.intersect(field.tagged_versions);
                        write_version_cond(
                            w,
                            valid_versions,
                            field.versions,
                            |w| {
                                write_encode_tag_buffer_inner(
                                    w,
                                    field,
                                    k,
                                    valid_versions,
                                    compute_size,
                                )
                            },
                            |w| write_fail_if_field_set(w, field),
                            false,
                            field.ignorable,
                        )?;
                        writeln!(w)?;
                        Ok(())
                    },
                    |_| Ok(()),
                    false,
                    true,
                )?;
            }
            writeln!(w)?;

            if compute_size {
                write!(w, "total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;")?;
            } else {
                write!(
                    w,
                    "write_unknown_tagged_fields(buf, {}.., &self.unknown_tagged_fields)?;",
                    current_tag + 1
                )?;
            }

            Ok(())
        },
        |_| Ok(()),
        false,
        true,
    )?;
    writeln!(w)?;
    Ok(())
}

fn write_encode_tag_buffer_inner<W: Write>(
    w: &mut CodeWriter<W>,
    field: &PreparedField,
    k: i32,
    valid_versions: VersionSpec,
    compute_size: bool,
) -> Result<(), Error> {
    let var_name = &field.var_name();
    let valid_versions = valid_versions.intersect(field.versions);
    let is_default = field.default.gen_is_default(var_name, field.optional).not();
    write!(w, "if {is_default} ")?;
    w.block(|w| {
        write!(w, "let computed_size = ")?;
        if !field.type_.has_compact_form() {
            write!(
                w,
                "{}.compute_size({})?",
                &field.type_.name(false, false),
                var_name
            )?;
        } else {
            write_version_cond(
                w,
                valid_versions,
                field.flexible_versions,
                |w| {
                    write!(
                        w,
                        "{}.compute_size({})?",
                        &field.type_.name(true, false),
                        var_name
                    )?;
                    Ok(())
                },
                |w| {
                    write!(
                        w,
                        "{}.compute_size({})?",
                        &field.type_.name(false, false),
                        var_name
                    )?;
                    Ok(())
                },
                false,
                false,
            )?;
        }
        writeln!(w, ";")?;
        write_size_check(
            w,
            "computed_size",
            "u32",
            "Tagged field is too large to encode ({} bytes)",
        )?;
        write_encode_or_compute(w, "types::UnsignedVarInt", k, compute_size)?;
        writeln!(w)?;
        write_encode_or_compute(
            w,
            "types::UnsignedVarInt",
            "computed_size as u32",
            compute_size,
        )?;
        writeln!(w)?;
        if compute_size {
            writeln!(w, "total_size += computed_size;")?;
            Ok(())
        } else if !field.type_.has_compact_form() {
            write_encode_or_compute(w, &field.type_.name(false, false), var_name, compute_size)
        } else {
            write_version_cond(
                w,
                valid_versions,
                field.flexible_versions,
                |w| {
                    write_encode_or_compute(
                        w,
                        &field.type_.name(true, false),
                        var_name,
                        compute_size,
                    )
                },
                |w| {
                    write_encode_or_compute(
                        w,
                        &field.type_.name(false, false),
                        var_name,
                        compute_size,
                    )
                },
                false,
                false,
            )
        }
    })
}

fn write_decode_field<W: Write>(
    w: &mut CodeWriter<W>,
    field: &PreparedField,
    valid_versions: VersionSpec,
) -> Result<(), Error> {
    let var_name = &field.name;

    if field.tagged_versions.is_none() {
        write!(w, "let {} = ", var_name)?;
    } else {
        write!(w, "let mut {} = ", var_name)?;
    }

    if field
        .tagged_versions
        .contains(field.versions.intersect(valid_versions))
    {
        writeln!(
            w,
            "{};",
            field
                .default
                .gen_default(field.optional, field.type_.is_entity())
        )?;
        return Ok(());
    }

    let optional = !field.nullable_versions.is_none();
    write_version_cond(
        w,
        valid_versions,
        field.tagged_versions,
        |w| {
            write!(
                w,
                "{}",
                field
                    .default
                    .gen_default(field.optional, field.type_.is_entity())
            )?;
            Ok(())
        },
        |w| {
            write_version_cond(
                w,
                valid_versions,
                field.versions,
                |w| {
                    let valid_versions = valid_versions.intersect(field.versions);
                    if !field.type_.has_compact_form() {
                        write!(w, "{}.decode(buf)?", field.type_.name(false, optional))?;
                    } else {
                        write_version_cond(
                            w,
                            valid_versions,
                            field.flexible_versions,
                            |w| {
                                write!(w, "{}.decode(buf)?", field.type_.name(true, optional))?;
                                Ok(())
                            },
                            |w| {
                                write!(w, "{}.decode(buf)?", field.type_.name(false, optional))?;
                                Ok(())
                            },
                            false,
                            false,
                        )?;
                    }
                    Ok(())
                },
                |w| {
                    write!(
                        w,
                        "{}",
                        field
                            .default
                            .gen_default(field.optional, field.type_.is_entity())
                    )?;
                    Ok(())
                },
                false,
                false,
            )
        },
        false,
        false,
    )?;
    writeln!(w, ";")?;

    Ok(())
}

fn write_decode_tag_buffer<W: Write>(
    w: &mut CodeWriter<W>,
    prepared_fields: &[PreparedField],
    valid_versions: VersionSpec,
    flexible_msg_versions: VersionSpec,
) -> Result<(), Error> {
    write_version_cond(
        w,
        valid_versions,
        flexible_msg_versions,
        |w| {
            let valid_versions = valid_versions.intersect(flexible_msg_versions);

            let sorted_tagged_fields: BTreeMap<i32, &PreparedField> = prepared_fields
                .iter()
                .filter_map(|field| Some((field.tag?, field)))
                .collect();

            writeln!(
                w,
                "let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;"
            )?;
            write!(w, "for _ in 0..num_tagged_fields ")?;
            w.block(|w| {
                writeln!(w, "let tag: u32 = types::UnsignedVarInt.decode(buf)?;")?;
                writeln!(w, "let size: u32 = types::UnsignedVarInt.decode(buf)?;")?;

                if sorted_tagged_fields.is_empty() {
                    writeln!(w, "let unknown_value = buf.try_get_bytes(size as usize)?;")?;
                    write!(
                        w,
                        "unknown_tagged_fields.insert(tag as i32, unknown_value);"
                    )?;
                    Ok(())
                } else {
                    write!(w, "match tag ")?;
                    w.block(|w| {
                        for (&k, field) in &sorted_tagged_fields {
                            let var_name = &field.name;
                            write!(w, "{} => ", k)?;
                            w.block(|w| {
                                let tagged_field_versions =
                                    field.tagged_versions.intersect(field.versions);
                                write_version_cond(
                                    w,
                                    valid_versions,
                                    tagged_field_versions,
                                    |w| {
                                        let valid_versions =
                                            valid_versions.intersect(tagged_field_versions);
                                        if !field.type_.has_compact_form() {
                                            write!(
                                                w,
                                                "{} = {}.decode(buf)?;",
                                                var_name,
                                                field.type_.name(false, false)
                                            )?;
                                        } else {
                                            write_version_cond(
                                                w,
                                                valid_versions,
                                                field.flexible_versions,
                                                |w| {
                                                    write!(
                                                        w,
                                                        "{} = {}.decode(buf)?;",
                                                        var_name,
                                                        field.type_.name(true, false)
                                                    )?;
                                                    Ok(())
                                                },
                                                |w| {
                                                    write!(
                                                        w,
                                                        "{} = {}.decode(buf)?;",
                                                        var_name,
                                                        field.type_.name(false, false)
                                                    )?;
                                                    Ok(())
                                                },
                                                false,
                                                false,
                                            )?;
                                        }
                                        Ok(())
                                    },
                                    |w| {
                                        writeln!(
                                            w,
                                            "bail!({:?}, tag, version);",
                                            "Tag {} is not valid for version {}"
                                        )?;
                                        Ok(())
                                    },
                                    false,
                                    false,
                                )
                            })?;
                            writeln!(w, ",")?;
                        }
                        write!(w, "_ => ")?;
                        w.block(|w| {
                            writeln!(w, "let unknown_value = buf.try_get_bytes(size as usize)?;")?;
                            write!(
                                w,
                                "unknown_tagged_fields.insert(tag as i32, unknown_value);"
                            )?;
                            Ok(())
                        })
                    })
                }
            })
        },
        |_| Ok(()),
        false,
        true,
    )?;
    writeln!(w)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn prepared_struct_def<W: Write>(
    w: &mut CodeWriter<W>,
    name: &str,
    fields: &[FieldSpec],
    entity_types: &mut BTreeSet<EntityType>,
    valid_versions: VersionSpec,
    flexible_msg_versions: VersionSpec,
    deprecated_versions: VersionSpec,
    prepared_structs: &BTreeMap<String, PreparedStruct>,
    prepared_structs_output: &mut Vec<PreparedStruct>,
    spec_type: SpecType,
) -> Result<PreparedStruct, Error> {
    let mut prepared_fields = Vec::new();

    for field in fields {
        let type_ = prepare_field_type(
            w,
            &field.type_,
            field,
            entity_types,
            valid_versions,
            flexible_msg_versions,
            deprecated_versions,
            prepared_structs,
            prepared_structs_output,
            spec_type,
        )?;

        let mut name = field.name.to_snake_case();
        if name == "type" {
            name = "_type".to_string();
        }
        if name == "match" {
            name = "_match".to_string();
        }

        let optional = !field.nullable_versions.is_none();
        let versions = field.versions.intersect(valid_versions);
        let flexible_versions = field.flexible_versions.unwrap_or(flexible_msg_versions);
        let tagged_versions = field.tagged_versions;
        let nullable_versions = field.nullable_versions;

        assert!(flexible_msg_versions.contains(flexible_versions));
        assert!(flexible_versions.contains(tagged_versions));

        let default = if let Some(default) = &field.default {
            let default_str = match default {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };

            match default_str.as_str() {
                "null" => {
                    assert!(nullable_versions.contains(field.versions));
                    PreparedDefault::Null
                }
                "true" | "false" => {
                    assert_eq!(type_, PreparedType::Primitive(PrimitiveType::Bool));
                    PreparedDefault::Boolean(default_str == "true")
                }
                _ => match &type_ {
                    PreparedType::Primitive(prim) => {
                        parse_primitive_default(*prim, default_str, &type_)
                    }
                    PreparedType::Entity(entity_type) => {
                        parse_primitive_default(entity_type.inner, default_str, &type_)
                    }
                    _ => panic!(
                        "Unexpected default value {:?} for {}",
                        default_str,
                        type_.rust_name()
                    ),
                },
            }
        } else {
            type_.default()
        };

        prepared_fields.push(PreparedField {
            name,
            optional,
            type_,
            versions,
            tag: field.tag,
            tagged_versions,
            nullable_versions: field.nullable_versions,
            default,
            ignorable: field.ignorable,
            _entity_type: field.entity_type.clone(),
            about: field.about.clone(),
            flexible_versions,
        });
    }

    let prepared_struct = PreparedStruct {
        spec_type,
        name: name.into(),
        prepared_fields,
        valid_versions,
        flexible_msg_versions,
        deprecated_versions,
    };
    prepared_structs_output.push(prepared_struct.clone());
    Ok(prepared_struct)
}

impl PreparedStruct {
    pub fn apply<W: Write>(&self, w: &mut CodeWriter<W>) -> Result<(), Error> {
        writeln!(w, "/// Valid versions: {}", self.valid_versions)?;
        writeln!(w, "#[non_exhaustive]")?;
        writeln!(w, "#[derive(Debug, Clone, PartialEq)]")?;
        write!(w, "pub struct {} ", self.name)?;
        w.block(|w| {
            for prepared_field in &self.prepared_fields {
                writeln!(w, "/// {}", prepared_field.about)?;
                writeln!(w, "/// ")?;
                writeln!(w, "/// Supported API versions: {}", prepared_field.versions)?;
                if prepared_field.optional {
                    writeln!(
                        w,
                        "pub {}: Option<{}>,",
                        prepared_field.name,
                        prepared_field.type_.rust_name()
                    )?;
                } else {
                    writeln!(
                        w,
                        "pub {}: {},",
                        prepared_field.name,
                        prepared_field.type_.rust_name()
                    )?;
                }
                writeln!(w)?;
            }

            if !self.flexible_msg_versions.is_none() {
                writeln!(w, "/// Other tagged fields")?;
                writeln!(w, "pub unknown_tagged_fields: BTreeMap<i32, Bytes>,")?;
            }

            Ok(())
        })?;
        writeln!(w)?;
        writeln!(w)?;

        write!(w, "impl {} ", self.name)?;
        w.block(|w| {
            for prepared_field in &self.prepared_fields {
                writeln!(w, "/// Sets `{}` to the passed value.", prepared_field.name)?;
                writeln!(w, "/// ")?;
                writeln!(w, "/// {}", prepared_field.about)?;
                writeln!(w, "/// ")?;
                writeln!(w, "/// Supported API versions: {}", prepared_field.versions)?;
                if prepared_field.optional {
                    writeln!(
                        w,
                        "pub fn with_{}(mut self, value: Option<{}>) -> Self",
                        prepared_field.name.trim_start_matches('_'),
                        prepared_field.type_.rust_name()
                    )?;
                } else {
                    writeln!(
                        w,
                        "pub fn with_{}(mut self, value: {}) -> Self",
                        prepared_field.name.trim_start_matches('_'),
                        prepared_field.type_.rust_name()
                    )?;
                }

                w.block(|w| {
                    writeln!(w, "self.{} = value;", prepared_field.name)?;
                    writeln!(w, "self")?;
                    Ok(())
                })?;
            }

            if !self.flexible_msg_versions.is_none() {
                writeln!(w, "/// Sets unknown_tagged_fields to the passed value.")?;
                writeln!(
                    w,
                    "pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self"
                )?;
                w.block(|w| {
                    writeln!(w, "self.unknown_tagged_fields = value;")?;
                    writeln!(w, "self")?;

                    Ok(())
                })?;

                writeln!(w, "/// Inserts an entry into unknown_tagged_fields.")?;
                writeln!(
                    w,
                    "pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self"
                )?;
                w.block(|w| {
                    writeln!(w, "self.unknown_tagged_fields.insert(key, value);")?;
                    writeln!(w, "self")?;

                    Ok(())
                })?;
            }

            Ok(())
        })?;
        writeln!(w)?;
        writeln!(w)?;

        match self.spec_type {
            SpecType::Request => writeln!(w, "#[cfg(feature = \"client\")]")?,
            SpecType::Response => writeln!(w, "#[cfg(feature = \"broker\")]")?,
            _ => {}
        }

        write!(w, "impl Encodable for {} ", self.name)?;
        w.block(|w| {
            write!(
                w,
                "fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> "
            )?;
            w.block(|w| {
                self.write_version_validation(w)?;

                for prepared_field in &self.prepared_fields {
                    write_encode_field(w, prepared_field, self.valid_versions, false)?;
                }
                write_encode_tag_buffer(
                    w,
                    &self.prepared_fields,
                    self.valid_versions,
                    self.flexible_msg_versions,
                    false,
                )?;
                write!(w, "Ok(())")?;
                Ok(())
            })?;
            writeln!(w)?;
            write!(w, "fn compute_size(&self, version: i16) -> Result<usize> ")?;
            w.block(|w| {
                writeln!(w, "let mut total_size = 0;")?;
                for prepared_field in &self.prepared_fields {
                    write_encode_field(w, prepared_field, self.valid_versions, true)?;
                }
                write_encode_tag_buffer(
                    w,
                    &self.prepared_fields,
                    self.valid_versions,
                    self.flexible_msg_versions,
                    true,
                )?;
                write!(w, "Ok(total_size)")?;
                Ok(())
            })?;
            Ok(())
        })?;
        writeln!(w)?;
        writeln!(w)?;

        match self.spec_type {
            SpecType::Request => writeln!(w, "#[cfg(feature = \"broker\")]")?,
            SpecType::Response => writeln!(w, "#[cfg(feature = \"client\")]")?,
            _ => {}
        }
        write!(w, "impl Decodable for {} ", self.name)?;
        w.block(|w| {
            write!(
                w,
                "fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> "
            )?;
            w.block(|w| {
                self.write_version_validation(w)?;
                for prepared_field in &self.prepared_fields {
                    write_decode_field(w, prepared_field, self.valid_versions)?;
                }
                if !self.flexible_msg_versions.is_none() {
                    writeln!(w, "let mut unknown_tagged_fields = BTreeMap::new();")?;
                    write_decode_tag_buffer(
                        w,
                        &self.prepared_fields,
                        self.valid_versions,
                        self.flexible_msg_versions,
                    )?;
                }
                write!(w, "Ok(Self ")?;
                w.block(|w| {
                    for prepared_field in &self.prepared_fields {
                        writeln!(w, "{},", prepared_field.name)?;
                    }

                    if !self.flexible_msg_versions.is_none() {
                        writeln!(w, "unknown_tagged_fields,")?;
                    }

                    Ok(())
                })?;

                write!(w, ")")?;
                Ok(())
            })
        })?;
        writeln!(w)?;
        writeln!(w)?;

        write!(w, "impl Default for {} ", self.name)?;
        w.block(|w| {
            write!(w, "fn default() -> Self ")?;
            w.block(|w| {
                write!(w, "Self ")?;
                w.block(|w| {
                    for prepared_field in &self.prepared_fields {
                        writeln!(
                            w,
                            "{}: {},",
                            prepared_field.name,
                            prepared_field.default.gen_default(
                                prepared_field.optional,
                                prepared_field.type_.is_entity(),
                            )
                        )?;
                    }

                    if !self.flexible_msg_versions.is_none() {
                        writeln!(w, "unknown_tagged_fields: BTreeMap::new(),")?;
                    }

                    Ok(())
                })
            })
        })?;
        writeln!(w)?;
        writeln!(w)?;

        write!(w, "impl Message for {} ", self.name)?;
        w.block(|w| {
            let range = self.valid_versions
                .range()
                .expect("Valid versions should be bounded.");
            writeln!(
                w,
                "const VERSIONS: VersionRange = VersionRange {{ min: {}, max: {} }};",
                range.start(),
                range.end()
            )?;
            if let Some(range) = self.deprecated_versions.range() {
                writeln!(
                    w,
                    "const DEPRECATED_VERSIONS: Option<VersionRange> = Some(VersionRange {{ min: {}, max: {} }});",
                    range.start(),
                    range.end()
                )?;
            } else {
                writeln!(w, "const DEPRECATED_VERSIONS: Option<VersionRange> = None;")?;
            }
            Ok(())
        })?;
        writeln!(w)?;
        writeln!(w)?;

        Ok(())
    }

    fn write_version_validation<W: Write>(&self, w: &mut CodeWriter<W>) -> Result<(), Error> {
        match self.valid_versions {
            VersionSpec::None => {}
            VersionSpec::Exact(exact) => {
                writeln!(w, "if version != {exact} {{")?;
                writeln!(
                    w,
                    "    bail!(\"specified version not supported by this message type\");"
                )?;
                writeln!(w, "}}")?;
            }
            VersionSpec::Since(since) => {
                writeln!(w, "if version >= {since} {{")?;
                writeln!(
                    w,
                    "    bail!(\"specified version not supported by this message type\");"
                )?;
                writeln!(w, "}}")?;
            }
            VersionSpec::Range(start, end) => {
                writeln!(w, "if version < {start} || version > {end} {{")?;
                writeln!(
                    w,
                    "    bail!(\"specified version not supported by this message type\");"
                )?;
                writeln!(w, "}}")?;
            }
        }
        Ok(())
    }
}

fn write_file_header<W: Write>(w: &mut CodeWriter<W>, name: &str) -> Result<(), Error> {
    writeln!(w, "//! {}", name)?;
    writeln!(w, "//!")?;
    writeln!(w, "//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/{}.json).", name)?;
    writeln!(
        w,
        "// WARNING: the items of this module are generated and should not be edited directly"
    )?;
    writeln!(w, "#![allow(unused)]")?;
    writeln!(w)?;
    writeln!(w, "use std::borrow::Borrow;")?;
    writeln!(w, "use std::collections::BTreeMap;")?;
    writeln!(w)?;
    writeln!(w, "use bytes::Bytes;")?;
    writeln!(w, "use uuid::Uuid;")?;
    writeln!(w, "use crate::error::{{bail, Result}};")?;
    writeln!(w)?;
    writeln!(w, "use crate::protocol::{{")?;
    writeln!(
        w,
        "    Encodable, Decodable, Encoder, Decoder, Message, HeaderVersion, VersionRange,"
    )?;
    writeln!(w, "    types, write_unknown_tagged_fields, compute_unknown_tagged_fields_size, StrBytes, buf::{{ByteBuf, ByteBufMut}}")?;
    writeln!(w, "}};")?;
    writeln!(w)?;
    writeln!(w)?;
    Ok(())
}

pub struct GenerationOutput {
    buffer: Vec<u8>,
    pub file_name: String,
    pub module_name: String,
    pub struct_name: String,
    pub entity_types: BTreeSet<EntityType>,
}

impl GenerationOutput {
    fn new(
        buffer: Vec<u8>,
        file_name: String,
        module_name: String,
        struct_name: String,
        entity_types: BTreeSet<EntityType>,
    ) -> Self {
        Self {
            buffer,
            file_name,
            module_name,
            struct_name,
            entity_types,
        }
    }

    pub fn apply<W: Write>(
        &self,
        writer: &mut W,
        entity_types: &mut BTreeSet<EntityType>,
    ) -> std::io::Result<()> {
        let mut file = File::create(&self.file_name)?;
        file.write_all(&self.buffer)?;

        for entity in self.entity_types.iter() {
            if let Some(previous) = entity_types.replace(entity.clone()) {
                if previous.inner != entity.inner {
                    panic!(
                        "Entity type {} has conflicting definitions: {:?} and {:?}",
                        self.struct_name, previous, self.entity_types
                    );
                }
            }
        }

        writeln!(writer, "pub mod {};", self.module_name)?;
        writeln!(
            writer,
            "pub use {}::{};",
            self.module_name, self.struct_name
        )?;
        writeln!(writer)
    }
}

pub fn generate(output_path: &str, spec: Spec) -> Result<Option<GenerationOutput>, Error> {
    let struct_name = spec.name.clone();
    let module_name = struct_name.to_snake_case();
    let flexible_msg_versions = spec.flexible_versions;
    let deprecated_versions = spec.deprecated_versions;
    let mut entity_types: BTreeSet<EntityType> = BTreeSet::new();

    let valid_versions = if spec.latest_version_unstable {
        // skip unstable versions
        spec.valid_versions.without_last()
    } else {
        spec.valid_versions
    };

    // most likely, the spec has only one unstable version
    if valid_versions.is_none() {
        println!("Skipping generation of {struct_name} because it has no valid versions");
        return Ok(None);
    }

    let file_name = format!("{}/{}.rs", output_path, module_name);
    let mut buffer = Vec::new();
    let mut writer = CodeWriter::new(BufWriter::new(&mut buffer));

    write_file_header(&mut writer, &struct_name)?;

    let mut prepared_structs = BTreeMap::new();
    loop {
        let mut changed = false;
        let mut prepared_structs_output = Vec::new();

        for common_struct in &spec.common_structs {
            prepared_struct_def(
                &mut writer,
                &common_struct.name,
                &common_struct.fields,
                &mut entity_types,
                valid_versions,
                flexible_msg_versions,
                deprecated_versions,
                &prepared_structs,
                &mut prepared_structs_output,
                spec.type_,
            )?;
        }

        prepared_struct_def(
            &mut writer,
            &struct_name,
            &spec.fields,
            &mut entity_types,
            valid_versions,
            flexible_msg_versions,
            deprecated_versions,
            &prepared_structs,
            &mut prepared_structs_output,
            spec.type_,
        )?;

        for prepared_struct in prepared_structs_output {
            if let Some(previous) =
                prepared_structs.insert(prepared_struct.name.clone(), prepared_struct.clone())
            {
                changed |= previous != prepared_struct;
            } else {
                changed = true;
            }
        }

        if !changed {
            break;
        }
    }
    for prepared_struct in prepared_structs.values() {
        prepared_struct.apply(&mut writer)?;
    }

    if let Some(api_key) = spec.api_key {
        write!(&mut writer, "impl HeaderVersion for {} ", struct_name)?;
        writer.block(|w| {
            write!(w, "fn header_version(version: i16) -> i16 ")?;
            w.block(|w| {
                match (spec.type_, api_key) {
                    // ApiVersionsResponse always includes a v0 header.
                    // See KIP-511 for details.
                    (SpecType::Response, 18) => {
                        write!(w, "0")?;
                    }
                    (SpecType::Request, _) => {
                        write_version_cond(
                            w,
                            valid_versions,
                            flexible_msg_versions,
                            |w| {
                                write!(w, "2")?;
                                Ok(())
                            },
                            |w| {
                                // Version 0 of ControlledShutdownRequest has a non-standard request header
                                // which does not include clientId.  Version 1 of ControlledShutdownRequest
                                // and later use the standard request header.
                                if api_key == 7 {
                                    write!(w, "if version == 0 {{ 0 }} else {{ 1 }}")?;
                                } else {
                                    write!(w, "1")?;
                                }
                                Ok(())
                            },
                            false,
                            false,
                        )?;
                    }
                    (SpecType::Response, _) => {
                        write_version_cond(
                            w,
                            valid_versions,
                            flexible_msg_versions,
                            |w| {
                                write!(w, "1")?;
                                Ok(())
                            },
                            |w| {
                                write!(w, "0")?;
                                Ok(())
                            },
                            false,
                            false,
                        )?;
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })
        })?;
        writeln!(&mut writer)?;
        writeln!(&mut writer)?;
    }

    drop(writer);
    Ok(Some(GenerationOutput::new(
        buffer,
        file_name,
        module_name,
        struct_name,
        entity_types,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_messages::spec::VersionSpec;

    #[test]
    fn write_version_cond_unknown_tagged() -> Result<(), Box<dyn std::error::Error>> {
        let mut cw = CodeWriter::new(Vec::new());

        // ex. request header
        let v1 = VersionSpec::Range(0, 2);
        let v2 = VersionSpec::Since(2);

        write_version_cond(
            &mut cw,
            v1,
            v2,
            |w| {
                write!(w, "true")?;
                Ok(())
            },
            |_| Ok(()),
            false,
            true,
        )?;

        assert_eq!(
            String::from_utf8(cw.into_inner()).unwrap(),
            "if version >= 2 {\n    true\n}".to_string()
        );
        Ok(())
    }
}
