//! Runtime behavior of `#[derive(JobArgs)]` expansions.

use std::time::Duration;

use riverqueue::{InsertOpts, JobArgs, JobState, ScheduleOverride, UniqueOpts};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JobArgs, Serialize)]
#[river(
    kind = "new_kind",
    aliases("old_kind", "older_kind"),
    max_attempts = 7,
    pending = true,
    priority = 3,
    queue = "critical_jobs",
    tags("billing", "priority-customer")
)]
struct ArgsWithDefaults {}

#[test]
fn provides_aliases_and_insert_defaults() {
    assert_eq!(ArgsWithDefaults::KIND, "new_kind");
    assert_eq!(ArgsWithDefaults::kind_aliases(), ["old_kind", "older_kind"]);
    let opts = ArgsWithDefaults::default_insert_opts();
    assert_eq!(opts.max_attempts(), Some(7));
    assert_eq!(opts.pending(), Some(true));
    assert_eq!(opts.priority(), Some(3));
    assert_eq!(opts.queue(), Some("critical_jobs"));
    assert_eq!(
        opts.tags(),
        Some(&["billing".to_owned(), "priority-customer".to_owned()][..])
    );
    assert!(opts.unique().is_none());
    assert!(ArgsWithDefaults::unique_fields().is_empty());
}

#[derive(Deserialize, JobArgs, Serialize)]
#[river(
    kind = "unique_invoice",
    unique(
        by_args("customer.id"),
        by_period = "1h30m",
        by_queue,
        by_state(available, pending, running, scheduled, retryable),
        exclude_kind
    )
)]
struct UniqueArgs {
    customer: Customer,
    #[river(unique)]
    #[serde(rename = "invoiceNumber")]
    invoice_number: String,
    note: String,
}

#[derive(Deserialize, Serialize)]
struct Customer {
    id: i64,
}

#[test]
fn declares_job_type_uniqueness() {
    let opts = UniqueArgs::default_insert_opts();
    let unique = opts.unique().expect("unique options");
    assert!(unique.uses_args());
    assert_eq!(unique.period(), Some(Duration::from_mins(90)));
    assert!(unique.uses_queue());
    assert_eq!(
        unique.states(),
        Some(
            &[
                JobState::Available,
                JobState::Pending,
                JobState::Running,
                JobState::Scheduled,
                JobState::Retryable,
            ][..]
        )
    );
    assert!(unique.excludes_kind());
    assert_eq!(
        UniqueArgs::unique_fields(),
        [&["invoiceNumber"][..], &["customer", "id"][..]]
    );
}

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "literal_paths", unique(by_args("user.id", "user\\.id")))]
struct LiteralPaths {
    #[river(unique)]
    #[serde(rename = "@user")]
    at: String,
    #[river(unique)]
    #[serde(rename = ":id")]
    colon: String,
    #[serde(rename = "user.id")]
    literal: String,
    user: Customer,
    #[river(unique)]
    #[serde(rename = "é")]
    unicode: String,
}

#[test]
fn separates_literal_and_nested_unique_fields() {
    assert_eq!(
        LiteralPaths::unique_fields(),
        [
            &["@user"][..],
            &[":id"][..],
            &["é"][..],
            &["user", "id"][..],
            &["user.id"][..],
        ]
    );
}

#[derive(Deserialize, JobArgs, Serialize)]
#[river(kind = "all_args_unique", unique(by_args))]
struct AllArgsUnique {
    value: String,
}

#[test]
fn by_args_without_fields_hashes_every_argument() {
    let opts = AllArgsUnique::default_insert_opts();
    assert!(opts.unique().is_some_and(UniqueOpts::uses_args));
    assert!(AllArgsUnique::unique_fields().is_empty());
}

fn overlaid_insert_opts() -> InsertOpts {
    InsertOpts::default()
        .with_priority(2)
        .with_unique(UniqueOpts::new().by_queue())
}

#[derive(Deserialize, JobArgs, Serialize)]
#[river(
    kind = "overlaid",
    priority = 4,
    queue = "attribute_queue",
    unique(by_args),
    insert_opts = overlaid_insert_opts
)]
struct OverlaidArgs {}

#[test]
fn insert_opts_function_overlays_attribute_defaults() {
    let opts = OverlaidArgs::default_insert_opts();
    // The function's options win; the attribute's remain where it sets none.
    assert_eq!(opts.priority(), Some(2));
    assert_eq!(opts.queue(), Some("attribute_queue"));
    assert_eq!(opts.scheduled_at(), ScheduleOverride::Inherit);
    let unique = opts.unique().expect("unique options");
    assert!(unique.uses_queue());
    assert!(!unique.uses_args());
}

mod reexport {
    pub use riverqueue as river;
}

#[derive(Deserialize, JobArgs, Serialize)]
#[river(
    kind = "renamed_crate",
    crate = "reexport::river",
    unique(by_state(available, pending, running, scheduled))
)]
struct RenamedCrateArgs {}

#[test]
fn crate_attribute_selects_the_riverqueue_path() {
    assert_eq!(RenamedCrateArgs::KIND, "renamed_crate");
    assert_eq!(
        RenamedCrateArgs::default_insert_opts()
            .unique()
            .and_then(UniqueOpts::states)
            .map(<[JobState]>::len),
        Some(4)
    );
}
