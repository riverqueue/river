//! River must not change `serde_json` semantics for the rest of an
//! application. Cargo unifies features across a build, so enabling
//! `serde_json`'s `arbitrary_precision` or `preserve_order` anywhere in the
//! workspace would silently alter unrelated user code. These tests fail if
//! either feature is enabled.

use riverqueue::{JobArgs, JobRow, encoding::encode_args};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize, JobArgs, PartialEq, Serialize)]
#[river(kind = "flattened_float")]
struct FlattenedArgs {
    #[serde(flatten)]
    inner: Inner,
    label: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Inner {
    amount: f64,
}

#[test]
fn flattened_float_deserializes_with_river_in_the_graph() {
    // `arbitrary_precision` breaks numbers inside `#[serde(flatten)]` with
    // "invalid type: map, expected f64".
    let decoded: FlattenedArgs = serde_json::from_str(r#"{"amount":1.5,"label":"x"}"#).unwrap();
    assert_eq!(decoded.inner.amount.to_bits(), 1.5_f64.to_bits());
}

#[test]
fn flattened_job_args_round_trip_through_a_job_row() {
    let args = FlattenedArgs {
        inner: Inner { amount: 0.25 },
        label: "invoice".to_owned(),
    };
    let row = JobRow::new(
        1,
        FlattenedArgs::KIND,
        encode_args(&args).unwrap(),
        chrono::Utc::now(),
    );

    assert_eq!(
        row.encoded_args.get(),
        r#"{"amount":0.25,"label":"invoice"}"#
    );
    assert_eq!(row.decode_args::<FlattenedArgs>().unwrap(), args);
}

#[test]
fn serde_json_defaults_are_unchanged() {
    // Without `arbitrary_precision`, numbers are parsed into `f64`.
    let value: Value = serde_json::from_str("1.10").unwrap();
    assert_eq!(value.to_string(), "1.1");

    // Without `preserve_order`, `Map` iterates in key order.
    let map: Map<String, Value> = serde_json::from_str(r#"{"b":1,"a":2}"#).unwrap();
    assert_eq!(map.keys().collect::<Vec<_>>(), ["a", "b"]);
}
