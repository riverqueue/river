use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_args("customer.id")))]
struct InvoiceArgs {
    invoice_number: String,
}

fn main() {}
