use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_period = "500ms"))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
