//! Type conversions between HMS Thrift types and Sail catalog types
//!
//! This module handles conversion between Hive Metastore Thrift structures
//! and Sail's internal catalog representation.

use crate::error::{HmsError, HmsResult};
use arrow_schema::DataType;
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Convert HMS type string to Arrow DataType
pub fn parse_hms_type(type_str: &str) -> HmsResult<DataType> {
    let type_str = type_str.trim().to_lowercase();

    match type_str.as_str() {
        // Primitive types
        "boolean" => Ok(DataType::Boolean),
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "timestamp" => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
        "date" => Ok(DataType::Date32),

        // Complex types requiring parsing
        s if s.starts_with("decimal") => parse_decimal_type(s),
        s if s.starts_with("char") || s.starts_with("varchar") => parse_char_type(s),
        s if s.starts_with("array") => parse_array_type(s),
        s if s.starts_with("map") => parse_map_type(s),
        s if s.starts_with("struct") => parse_struct_type(s),

        // Fallback
        _ => Ok(DataType::Utf8), // Default to string for unknown types
    }
}

/// Parse decimal type (e.g., "decimal(10,2)")
fn parse_decimal_type(type_str: &str) -> HmsResult<DataType> {
    static DECIMAL_REGEX: OnceLock<Regex> = OnceLock::new();
    let re = DECIMAL_REGEX.get_or_init(|| {
        Regex::new(r"decimal\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)").unwrap()
    });

    if let Some(captures) = re.captures(type_str) {
        let precision: u8 = captures
            .get(1)
            .unwrap()
            .as_str()
            .parse()
            .map_err(|_| HmsError::TypeConversion("Invalid decimal precision".into()))?;

        let scale: i8 = captures
            .get(2)
            .map(|m| m.as_str().parse().ok())
            .flatten()
            .unwrap_or(0);

        Ok(DataType::Decimal128(precision, scale))
    } else {
        // Default decimal
        Ok(DataType::Decimal128(10, 0))
    }
}

/// Parse char/varchar type (e.g., "varchar(100)")
fn parse_char_type(type_str: &str) -> HmsResult<DataType> {
    // Both char and varchar map to Utf8 in Arrow
    Ok(DataType::Utf8)
}

/// Parse array type (e.g., "array<int>")
fn parse_array_type(type_str: &str) -> HmsResult<DataType> {
    static ARRAY_REGEX: OnceLock<Regex> = OnceLock::new();
    let re = ARRAY_REGEX
        .get_or_init(|| Regex::new(r"array\s*<\s*(.+)\s*>").unwrap());

    if let Some(captures) = re.captures(type_str) {
        let element_type_str = captures.get(1).unwrap().as_str();
        let element_type = parse_hms_type(element_type_str)?;

        Ok(DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            element_type,
            true,
        ))))
    } else {
        Err(HmsError::TypeConversion(format!(
            "Invalid array type: {}",
            type_str
        )))
    }
}

/// Parse map type (e.g., "map<string,int>")
fn parse_map_type(type_str: &str) -> HmsResult<DataType> {
    static MAP_REGEX: OnceLock<Regex> = OnceLock::new();
    let re = MAP_REGEX
        .get_or_init(|| Regex::new(r"map\s*<\s*(.+?)\s*,\s*(.+)\s*>").unwrap());

    if let Some(captures) = re.captures(type_str) {
        let key_type_str = captures.get(1).unwrap().as_str();
        let value_type_str = captures.get(2).unwrap().as_str();

        let key_type = parse_hms_type(key_type_str)?;
        let value_type = parse_hms_type(value_type_str)?;

        // Arrow Map type
        Ok(DataType::Map(
            Arc::new(arrow_schema::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        arrow_schema::Field::new("key", key_type, false),
                        arrow_schema::Field::new("value", value_type, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ))
    } else {
        Err(HmsError::TypeConversion(format!(
            "Invalid map type: {}",
            type_str
        )))
    }
}

/// Parse struct type (e.g., "struct<name:string,age:int>")
fn parse_struct_type(type_str: &str) -> HmsResult<DataType> {
    static STRUCT_REGEX: OnceLock<Regex> = OnceLock::new();
    let re = STRUCT_REGEX
        .get_or_init(|| Regex::new(r"struct\s*<\s*(.+)\s*>").unwrap());

    if let Some(captures) = re.captures(type_str) {
        let fields_str = captures.get(1).unwrap().as_str();

        // Parse fields (simplified - doesn't handle nested commas)
        let fields: HmsResult<Vec<arrow_schema::Field>> = fields_str
            .split(',')
            .map(|field_str| {
                let parts: Vec<&str> = field_str.trim().splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(HmsError::TypeConversion(format!(
                        "Invalid struct field: {}",
                        field_str
                    )));
                }

                let field_name = parts[0].trim();
                let field_type = parse_hms_type(parts[1].trim())?;

                Ok(arrow_schema::Field::new(field_name, field_type, true))
            })
            .collect();

        Ok(DataType::Struct(fields?.into()))
    } else {
        Err(HmsError::TypeConversion(format!(
            "Invalid struct type: {}",
            type_str
        )))
    }
}

/// Normalize optional string (trim and convert empty to None)
pub fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Escape partition value for HMS
pub fn escape_partition_value(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Unescape partition value from HMS
pub fn unescape_partition_value(value: &str) -> HmsResult<String> {
    urlencoding::decode(value)
        .map(|s| s.into_owned())
        .map_err(|e| HmsError::TypeConversion(format!("Invalid partition value: {}", e)))
}

/// Parse partition spec string (e.g., "year=2023/month=01")
pub fn parse_partition_spec(spec: &str) -> HmsResult<HashMap<String, String>> {
    spec.split('/')
        .map(|part| {
            let mut kv = part.splitn(2, '=');
            let key = kv
                .next()
                .ok_or_else(|| HmsError::TypeConversion("Missing partition key".into()))?;
            let value = kv
                .next()
                .ok_or_else(|| HmsError::TypeConversion("Missing partition value".into()))?;

            Ok((key.to_string(), unescape_partition_value(value)?))
        })
        .collect()
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_primitive_types() {
        assert_eq!(parse_hms_type("boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_hms_type("int").unwrap(), DataType::Int32);
        assert_eq!(parse_hms_type("bigint").unwrap(), DataType::Int64);
        assert_eq!(parse_hms_type("double").unwrap(), DataType::Float64);
        assert_eq!(parse_hms_type("string").unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_parse_decimal() {
        let dt = parse_hms_type("decimal(10,2)").unwrap();
        assert_eq!(dt, DataType::Decimal128(10, 2));

        let dt = parse_hms_type("decimal(18)").unwrap();
        assert_eq!(dt, DataType::Decimal128(18, 0));
    }

    #[test]
    fn test_parse_array() {
        let dt = parse_hms_type("array<int>").unwrap();
        match dt {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Int32);
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn test_normalize_optional_string() {
        assert_eq!(normalize_optional_string(None), None);
        assert_eq!(normalize_optional_string(Some("".to_string())), None);
        assert_eq!(normalize_optional_string(Some("  ".to_string())), None);
        assert_eq!(
            normalize_optional_string(Some("test".to_string())),
            Some("test".to_string())
        );
    }

    #[test]
    fn test_partition_value_escaping() {
        let original = "value/with=special&chars";
        let escaped = escape_partition_value(original);
        let unescaped = unescape_partition_value(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_parse_partition_spec() {
        let spec = "year=2023/month=01/day=15";
        let result = parse_partition_spec(spec).unwrap();

        assert_eq!(result.get("year"), Some(&"2023".to_string()));
        assert_eq!(result.get("month"), Some(&"01".to_string()));
        assert_eq!(result.get("day"), Some(&"15".to_string()));
    }
}
