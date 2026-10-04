//! A serializer that only checks a value for non-finite floats.
//!
//! [`serde_json`] writes `NaN` and infinities as `null` through the same
//! formatter call as a real `null`, so the check can't happen in
//! [`GoFormatter`](super::GoFormatter). Go's `encoding/json` rejects these
//! values with an `UnsupportedValueError`, so River walks the value once with
//! this serializer before encoding it.

use std::fmt;

use serde::{Serialize, ser};

/// Returns an error like Go's `json: unsupported value: NaN` when `value`
/// contains a non-finite float anywhere.
pub(crate) fn check<T: Serialize + ?Sized>(value: &T) -> Result<(), serde_json::Error> {
    value
        .serialize(FiniteCheck)
        .map_err(|error| <serde_json::Error as ser::Error>::custom(error.0))
}

#[derive(Debug)]
pub(crate) struct CheckError(String);

impl fmt::Display for CheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CheckError {}

impl ser::Error for CheckError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

fn check_float(value: f64) -> Result<(), CheckError> {
    if value.is_finite() {
        return Ok(());
    }
    // Go formats the value with `strconv.FormatFloat(v, 'g', -1, bits)`.
    let formatted = if value.is_nan() {
        "NaN"
    } else if value.is_sign_positive() {
        "+Inf"
    } else {
        "-Inf"
    };
    Err(CheckError(format!("unsupported value: {formatted}")))
}

struct FiniteCheck;

impl ser::Serializer for FiniteCheck {
    type Error = CheckError;
    type Ok = ();
    type SerializeMap = Self;
    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;

    fn serialize_bool(self, _: bool) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_i8(self, _: i8) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_i16(self, _: i16) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_i32(self, _: i32) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_i64(self, _: i64) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_i128(self, _: i128) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_u8(self, _: u8) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_u16(self, _: u16) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_u32(self, _: u32) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_u64(self, _: u64) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_u128(self, _: u128) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), CheckError> {
        check_float(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> Result<(), CheckError> {
        check_float(value)
    }

    fn serialize_char(self, _: char) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_str(self, _: &str) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_none(self) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), CheckError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), CheckError> {
        Ok(())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), CheckError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), CheckError> {
        value.serialize(self)
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_tuple(self, _: usize) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, CheckError> {
        Ok(self)
    }

    fn collect_str<T: fmt::Display + ?Sized>(self, _: &T) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeSeq for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeTuple for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeMap for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CheckError> {
        key.serialize(Self)
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeStruct for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for FiniteCheck {
    type Error = CheckError;
    type Ok = ();

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), CheckError> {
        value.serialize(Self)
    }

    fn end(self) -> Result<(), CheckError> {
        Ok(())
    }
}
