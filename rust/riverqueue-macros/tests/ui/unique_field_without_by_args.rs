use riverqueue::JobArgs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "invoice", unique(by_queue))]
struct InvoiceArgs {
    #[river(unique)]
    invoice_number: String,
}

fn main() {}
