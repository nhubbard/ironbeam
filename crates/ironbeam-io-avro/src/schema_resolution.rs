//! Apply reader field aliases before the Avro value resolver.

use apache_avro::{Schema, types::Value};
use ironbeam_core::Error;

fn apply_union_aliases(
    value: &mut Value,
    union: &apache_avro::schema::UnionSchema,
) -> Result<(), Error> {
    let inner = match value {
        Value::Union(_, inner) => inner.as_mut(),
        other => other,
    };
    let mut alias_error = None;
    for variant in union.variants() {
        let mut candidate = inner.clone();
        if let Err(error) = apply_field_aliases(&mut candidate, variant) {
            alias_error = Some(error);
            continue;
        }
        if candidate.clone().resolve(variant).is_ok() {
            *inner = candidate;
            return Ok(());
        }
    }
    if let Some(error) = alias_error {
        return Err(error);
    }
    Ok(())
}

fn apply_field_aliases(value: &mut Value, schema: &Schema) -> Result<(), Error> {
    match (value, schema) {
        (value, Schema::Union(union)) => apply_union_aliases(value, union)?,
        (Value::Record(values), Schema::Record(record)) => {
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
                    values[index].0.clone_from(&field.name);
                }
            }
            for field in &record.fields {
                if let Some((_, nested)) = values.iter_mut().find(|(name, _)| name == &field.name) {
                    apply_field_aliases(nested, &field.schema)?;
                }
            }
        }
        (Value::Array(values), Schema::Array(array)) => {
            for value in values {
                apply_field_aliases(value, &array.items)?;
            }
        }
        (Value::Map(values), Schema::Map(map)) => {
            for value in values.values_mut() {
                apply_field_aliases(value, &map.types)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn resolve(mut value: Value, schema: &Schema) -> Result<Value, Error> {
    apply_field_aliases(&mut value, schema)?;
    value
        .resolve(schema)
        .map_err(|error| Error::io("avro", "schema", error.to_string()))
}
