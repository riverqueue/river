use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique("invoice_number"))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
