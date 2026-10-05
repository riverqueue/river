use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(queue = "invoices")]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
