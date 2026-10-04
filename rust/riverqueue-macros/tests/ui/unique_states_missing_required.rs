use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_state(available, running)))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
