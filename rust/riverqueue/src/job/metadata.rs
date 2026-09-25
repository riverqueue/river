//! Exact persisted job metadata.

use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::{Map, Value, value::RawValue};

/// A JSON object whose original value tokens remain intact.
///
/// PostgreSQL may store numbers beyond the range of `serde_json::Value`, so
/// decoding the entire object into a map can make an otherwise valid job
/// unreadable. This type keeps the persisted text and decodes only requested
/// fields. Database-side metadata merges preserve unrelated value tokens.
#[derive(Clone)]
pub struct JobMetadata(Box<RawValue>);

impl JobMetadata {
    /// Accepts `raw` only when it holds a JSON object. Every fallible
    /// conversion goes through here so they reject the same inputs with the
    /// same error.
    fn from_raw_object(raw: Box<RawValue>) -> Result<Self, serde_json::Error> {
        if raw.get().trim_start().starts_with('{') {
            Ok(Self(raw))
        } else {
            Err(<serde_json::Error as serde::de::Error>::custom(
                "job metadata must be a JSON object",
            ))
        }
    }

    /// Returns the exact stored JSON object.
    #[must_use]
    pub fn as_raw(&self) -> &RawValue {
        &self.0
    }

    /// Returns whether the object contains a field, without decoding values.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.get_raw(key).is_some()
    }

    /// Decodes one field into a caller-selected type.
    ///
    /// # Errors
    ///
    /// Returns an error if the selected field cannot deserialize as `T`.
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, serde_json::Error> {
        self.get_raw(key)
            .map(|raw| serde_json::from_str(raw.get()))
            .transpose()
    }

    /// Borrows one field's original JSON value, resolving duplicate names to
    /// their last occurrence as Go's `encoding/json` does.
    #[must_use]
    pub fn get_raw(&self, key: &str) -> Option<&RawValue> {
        // Borrowed RawValue skips the number parser, including numbers much
        // larger than f64. The map owns only field names, not value bytes.
        let fields: BTreeMap<String, &RawValue> = serde_json::from_str(self.0.get()).ok()?;
        fields.get(key).copied()
    }

    /// Returns whether this object has no fields.
    ///
    /// # Panics
    ///
    /// Panics only if the internally validated JSON object becomes malformed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let fields: BTreeMap<String, &RawValue> =
            serde_json::from_str(self.0.get()).expect("validated metadata object");
        fields.is_empty()
    }

    /// Sets a field while preserving every other field's original value text
    /// and relative order.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` cannot be serialized as JSON.
    ///
    /// # Panics
    ///
    /// Panics only if the internally validated JSON object becomes malformed.
    pub fn insert(&mut self, key: &str, value: impl Serialize) -> Result<(), serde_json::Error> {
        let encoded_key = serde_json::to_string(key)?;
        let encoded_value = serde_json::to_string(&value)?;
        let members = crate::unique::object_members(self.0.get()).expect("validated object");
        let last = members.iter().rposition(|member| member.key == key);
        let mut result = String::from("{");
        for (index, member) in members.iter().enumerate() {
            if index > 0 {
                result.push(',');
            }
            result.push_str(member.raw_key);
            result.push(':');
            result.push_str(if Some(index) == last {
                &encoded_value
            } else {
                member.value
            });
        }
        if last.is_none() {
            if !members.is_empty() {
                result.push(',');
            }
            result.push_str(&encoded_key);
            result.push(':');
            result.push_str(&encoded_value);
        }
        result.push('}');
        self.0 = RawValue::from_string(result).expect("valid object update");
        Ok(())
    }

    /// Consumes the metadata and returns its exact JSON object.
    #[must_use]
    pub fn into_raw(self) -> Box<RawValue> {
        self.0
    }

    /// Removes all occurrences of a field. Returns whether it was present.
    ///
    /// # Panics
    ///
    /// Panics only if the internally validated JSON object becomes malformed.
    pub fn remove(&mut self, key: &str) -> bool {
        let members = crate::unique::object_members(self.0.get()).expect("validated object");
        let mut result = String::from("{");
        let mut removed = false;
        for member in &members {
            if member.key == key {
                removed = true;
                continue;
            }
            if result.len() > 1 {
                result.push(',');
            }
            result.push_str(member.raw_key);
            result.push(':');
            result.push_str(member.value);
        }
        if removed {
            result.push('}');
            self.0 = RawValue::from_string(result).expect("valid object removal");
        }
        removed
    }

    /// Decodes the complete object when every number is representable by
    /// `serde_json::Value`.
    ///
    /// # Errors
    ///
    /// Returns an error for out-of-range numbers such as `1e400`.
    pub fn to_map(&self) -> Result<Map<String, Value>, serde_json::Error> {
        serde_json::from_str(self.0.get())
    }
}

impl Default for JobMetadata {
    fn default() -> Self {
        Self(RawValue::from_string("{}".to_owned()).expect("valid empty object"))
    }
}

impl fmt::Debug for JobMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("JobMetadata")
            .field(&self.0.get())
            .finish()
    }
}

impl fmt::Display for JobMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.get())
    }
}

impl From<Map<String, Value>> for JobMetadata {
    fn from(map: Map<String, Value>) -> Self {
        Self(
            RawValue::from_string(serde_json::to_string(&map).expect("JSON map serializes"))
                .expect("serialized JSON map is valid"),
        )
    }
}

impl FromStr for JobMetadata {
    type Err = serde_json::Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::from_raw_object(serde_json::from_str(text)?)
    }
}

impl PartialEq for JobMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

impl Eq for JobMetadata {}

impl Serialize for JobMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JobMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_raw_object(Box::<RawValue>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

impl TryFrom<Box<RawValue>> for JobMetadata {
    type Error = serde_json::Error;

    fn try_from(raw: Box<RawValue>) -> Result<Self, Self::Error> {
        Self::from_raw_object(raw)
    }
}

impl TryFrom<Value> for JobMetadata {
    type Error = serde_json::Error;

    /// Accepts a JSON object value. Because `Value` has already parsed its
    /// numbers, use [`FromStr`] or `TryFrom<Box<RawValue>>` to keep number
    /// text that `Value` can't represent exactly.
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Object(map) => Ok(map.into()),
            _ => Err(<serde_json::Error as serde::de::Error>::custom(
                "job metadata must be a JSON object",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_large_number_tokens_and_last_duplicate() {
        let metadata: JobMetadata = r#"{"n":1,"large":1e400,"n":2}"#.parse().unwrap();
        assert_eq!(metadata.as_raw().get(), r#"{"n":1,"large":1e400,"n":2}"#);
        assert_eq!(metadata.get_raw("large").unwrap().get(), "1e400");
        assert_eq!(metadata.get::<i32>("n").unwrap(), Some(2));
        assert!(metadata.to_map().is_err());
    }

    #[test]
    fn rejects_non_objects_through_every_conversion() {
        for text in ["[1]", "null", " 1"] {
            let parsed = text.parse::<JobMetadata>().unwrap_err();
            let deserialized = serde_json::from_str::<JobMetadata>(text).unwrap_err();
            let raw =
                JobMetadata::try_from(RawValue::from_string(text.to_owned()).unwrap()).unwrap_err();
            let value =
                JobMetadata::try_from(serde_json::from_str::<Value>(text).unwrap()).unwrap_err();
            for error in [parsed, deserialized, raw, value] {
                assert!(
                    error
                        .to_string()
                        .contains("job metadata must be a JSON object"),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn converts_json_object_values() {
        let metadata = JobMetadata::try_from(serde_json::json!({"a": 1})).unwrap();
        assert_eq!(metadata.as_raw().get(), r#"{"a":1}"#);
    }

    #[test]
    fn changes_one_field_without_round_tripping_other_numbers() {
        let mut metadata: JobMetadata = r#"{"z":1e400,"n":0.1000000000000000055511151231257827}"#
            .parse()
            .unwrap();
        metadata.insert("flag", true).unwrap();
        metadata.insert("n", 2).unwrap();
        assert_eq!(metadata.as_raw().get(), r#"{"z":1e400,"n":2,"flag":true}"#);
        assert!(metadata.remove("flag"));
        assert_eq!(metadata.as_raw().get(), r#"{"z":1e400,"n":2}"#);
    }
}
