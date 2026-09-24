//! Deduplicates jobs with unique options.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example unique
//! ```

use std::{error::Error, time::Duration};

use riverqueue::{Client, InsertOpts, JobArgs, UniqueOpts, migrate::PostgresMigrator};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Unique by default: at most one reconciliation per account per hour.
///
/// `#[river(unique)]` selects the arguments that identify a duplicate, so
/// jobs for the same account collide even when `requested_by` differs.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "reconcile_account", unique(by_args, by_period = "1h"))]
struct ReconcileAccount {
    #[river(unique)]
    account_id: i64,
    requested_by: String,
}

/// Not unique by default; individual insertions opt in with `InsertOpts`.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_digest")]
struct SendDigest {
    user_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;
    // An insert-only client needs no workers or queues.
    let client = Client::builder(pool).build()?;

    let first = client
        .insert(ReconcileAccount {
            account_id: 42,
            requested_by: "billing".to_owned(),
        })
        .await?;
    let second = client
        .insert(ReconcileAccount {
            account_id: 42,
            requested_by: "support".to_owned(),
        })
        .await?;
    // The second insertion returns the existing job instead of a new one.
    assert!(second.unique_skipped_as_duplicate);
    assert_eq!(first.id(), second.id());
    println!("reconcile_account deduplicated to job {}", first.id());

    let opts = || {
        InsertOpts::default().with_unique(
            UniqueOpts::new()
                .by_args()
                .by_period(Duration::from_hours(24)),
        )
    };
    let digest = client
        .insert(SendDigest { user_id: 7 })
        .opts(opts())
        .await?;
    let repeat = client
        .insert(SendDigest { user_id: 7 })
        .opts(opts())
        .await?;
    assert!(repeat.unique_skipped_as_duplicate);
    let other = client
        .insert(SendDigest { user_id: 8 })
        .opts(opts())
        .await?;
    // A different user is a different job. (Running the example again within
    // a day finds the jobs from the previous run instead.)
    assert_ne!(digest.id(), other.id());
    println!(
        "send_digest: user 7 -> job {}, user 8 -> job {}",
        digest.id(),
        other.id()
    );
    Ok(())
}
