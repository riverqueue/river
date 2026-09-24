use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_args("lines.*")))]
struct InvoiceArgs {
    lines: Vec<String>,
}

fn main() {}
