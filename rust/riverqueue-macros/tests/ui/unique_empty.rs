use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique())]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
