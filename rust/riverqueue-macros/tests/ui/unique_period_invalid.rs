use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_period = "1 hour"))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
