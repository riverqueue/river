use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_state(available, pending, running, scheduled, done)))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
