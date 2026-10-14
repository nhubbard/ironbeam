//! Apply reader field aliases before the Avro value resolver.

use apache_avro::{
    Schema,
    schema::{NamesRef, NamespaceRef, ResolvedSchema},
    types::Value,
};
use ironbeam_core::Error;

fn apply_union_aliases(
    value: &mut Value,
    union: &apache_avro::schema::UnionSchema,
    names: &NamesRef<'_>,
    namespace: NamespaceRef<'_>,
) -> Result<(), Error> {
    let inner = match value {
        Value::Union(_, inner) => inner.as_mut(),
        other => other,
    };
    let mut alias_error = None;
    for variant in union.variants() {
        let mut candidate = inner.clone();
        if let Err(error) = apply_field_aliases(&mut candidate, variant, names, namespace) {
            alias_error = Some(error);
            continue;
        }
        if candidate.clone().resolve_with_names(variant, names).is_ok() {
            *inner = candidate;
            return Ok(());
        }
    }
    if let Some(error) = alias_error {
        return Err(error);
    }
    Ok(())
}

fn apply_field_aliases(
    value: &mut Value,
    schema: &Schema,
    names: &NamesRef<'_>,
    namespace: NamespaceRef<'_>,
) -> Result<(), Error> {
    match (value, schema) {
        (value, Schema::Ref { name }) => {
            let name = name.fully_qualified_name(namespace);
            let target = names.get(name.as_ref()).ok_or_else(|| {
                Error::io(
                    "avro",
                    "schema",
                    format!("unresolved reader schema reference {name}"),
                )
            })?;
            apply_field_aliases(value, target, names, name.namespace())?;
        }
        (value, Schema::Union(union)) => apply_union_aliases(value, union, names, namespace)?,
        (Value::Record(values), Schema::Record(record)) => {
            let namespace = record
                .name
                .fully_qualified_name(namespace)
                .namespace()
                .map(str::to_owned);
            let mut renamed = Vec::new();
            for field in &record.fields {
                if values.iter().any(|(name, _)| name == &field.name) {
                    continue;
                }
                let mut matched = None;
                for (index, (name, _)) in values.iter().enumerate() {
                    if record.fields.iter().any(|primary| primary.name == *name) {
                        continue;
                    }
                    if field.aliases.contains(name) && matched.replace(index).is_some() {
                        return Err(Error::io(
                            "avro",
                            "schema",
                            format!("several writer fields match alias for {}", field.name),
                        ));
                    }
                }
                if let Some(index) = matched {
                    if renamed.iter().any(|(prior, _)| *prior == index) {
                        return Err(Error::io(
                            "avro",
                            "schema",
                            format!(
                                "several reader fields match writer field {}",
                                values[index].0
                            ),
                        ));
                    }
                    renamed.push((index, &field.name));
                }
            }
            for (index, name) in renamed {
                values[index].0.clone_from(name);
            }
            for field in &record.fields {
                if let Some((_, nested)) = values.iter_mut().find(|(name, _)| name == &field.name) {
                    apply_field_aliases(nested, &field.schema, names, namespace.as_deref())?;
                }
            }
        }
        (Value::Array(values), Schema::Array(array)) => {
            for value in values {
                apply_field_aliases(value, &array.items, names, namespace)?;
            }
        }
        (Value::Map(values), Schema::Map(map)) => {
            for value in values.values_mut() {
                apply_field_aliases(value, &map.types, names, namespace)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn resolve(mut value: Value, schema: &Schema) -> Result<Value, Error> {
    let resolved = ResolvedSchema::new(schema)
        .map_err(|error| Error::io("avro", "schema", error.to_string()))?;
    apply_field_aliases(&mut value, schema, resolved.get_names(), None)?;
    value
        .resolve_with_names(schema, resolved.get_names())
        .map_err(|error| Error::io("avro", "schema", error.to_string()))
}
