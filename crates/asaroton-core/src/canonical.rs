//! Canonical JSON helpers.
//!
//! Matrix signs and hashes the *canonical* form of JSON objects: keys sorted, no insignificant
//! whitespace, integers only within the ±2^53 range, no floats. `ruma` provides the value type;
//! these helpers convert to and from `serde_json` and bytes.

use ruma::{CanonicalJsonObject, CanonicalJsonValue, canonical_json::try_from_json_map};
use serde::Serialize;

use crate::{Error, Result};

/// Parse a JSON document into a canonical JSON object.
pub fn from_slice(bytes: &[u8]) -> Result<CanonicalJsonObject> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    from_json_value(value)
}

/// Parse a JSON string into a canonical JSON object.
pub fn from_str(s: &str) -> Result<CanonicalJsonObject> {
    from_slice(s.as_bytes())
}

/// Convert a `serde_json::Value` (which must be an object) into a canonical JSON object.
pub fn from_json_value(value: serde_json::Value) -> Result<CanonicalJsonObject> {
    match value {
        serde_json::Value::Object(map) => Ok(try_from_json_map(map)?),
        other => Err(Error::InvalidPdu(format!(
            "expected a JSON object, got {}",
            json_type_name(&other)
        ))),
    }
}

/// Serialize any value into a canonical JSON object.
pub fn from_serialize<T: Serialize>(value: &T) -> Result<CanonicalJsonObject> {
    match ruma::canonical_json::to_canonical_value(value)? {
        CanonicalJsonValue::Object(obj) => Ok(obj),
        other => Err(Error::InvalidPdu(format!(
            "expected a JSON object, got {}",
            canonical_type_name(&other)
        ))),
    }
}

/// The canonical string form of an object: sorted keys, minimal whitespace, raw UTF-8.
pub fn to_string(object: &CanonicalJsonObject) -> String {
    // `CanonicalJsonObject` is a `BTreeMap`, so key order is already sorted, and serde_json emits
    // non-ASCII characters unescaped, which is what canonical JSON requires.
    serde_json::to_string(object).unwrap_or_else(|_| "{}".to_owned())
}

/// Convert a canonical object back to a `serde_json::Value`.
pub fn to_json_value(object: CanonicalJsonObject) -> serde_json::Value {
    CanonicalJsonValue::Object(object).into()
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn canonical_type_name(v: &CanonicalJsonValue) -> &'static str {
    match v {
        CanonicalJsonValue::Null => "null",
        CanonicalJsonValue::Bool(_) => "boolean",
        CanonicalJsonValue::Integer(_) => "integer",
        CanonicalJsonValue::String(_) => "string",
        CanonicalJsonValue::Array(_) => "array",
        CanonicalJsonValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_keys_and_strips_whitespace() {
        let obj = from_str(r#"{ "b": 2, "a": { "d": [1, 2], "c": "x" } }"#).unwrap();
        assert_eq!(to_string(&obj), r#"{"a":{"c":"x","d":[1,2]},"b":2}"#);
    }

    #[test]
    fn keeps_unicode_unescaped() {
        // Spec example: non-ASCII must be emitted as UTF-8, not \u escapes.
        let obj = from_str(r#"{"a": "日本語"}"#).unwrap();
        assert_eq!(to_string(&obj), "{\"a\":\"日本語\"}");
    }

    #[test]
    fn rejects_floats() {
        assert!(from_str(r#"{"a": 1.5}"#).is_err());
    }

    #[test]
    fn rejects_non_objects() {
        assert!(from_str("[1,2]").is_err());
    }
}
