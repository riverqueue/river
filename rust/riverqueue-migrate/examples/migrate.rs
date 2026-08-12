use std::error::Error;

use riverqueue_migrate::{Direction, MigrateOpts, PostgresMigrator};
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let migrator = PostgresMigrator::new(pool);

    let preview = migrator
        .migrate(Direction::Up, MigrateOpts::new().with_dry_run(true))
        .await?;
    for migration in preview.versions {
        println!("would apply {:03} {}", migration.version, migration.name);
    }

    migrator.migrate_up().await?;
    let validation = migrator.validate(None).await?;
    if !validation.ok {
        return Err(validation.messages.join("; ").into());
    }
    Ok(())
}
