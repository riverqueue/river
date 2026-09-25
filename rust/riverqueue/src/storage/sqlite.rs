//! SQLite implementation of River's storage operations.

use chrono::Utc;
use serde_json::{Map, Value};
use sqlx::SqliteConnection;

use super::Backend;
use crate::__private::{DatabaseConnection, ExtensionClaimParams};
use crate::database::sqlite;
use crate::{Error, JobListParams, JobRow, JobState, Queue};

/// SQLite storage bound to one connection.
pub(super) struct SqliteBackend<'c> {
    pub(super) connection: &'c mut SqliteConnection,
}

impl SqliteBackend<'_> {
    /// Lists jobs, optionally excluding running jobs before the limit
    /// applies as bulk deletion does.
    async fn list(
        &mut self,
        params: &JobListParams,
        exclude_running: bool,
    ) -> Result<Vec<JobRow>, Error> {
        let kinds = params.kinds.iter().map(String::as_str).collect::<Vec<_>>();
        let queues = params.queues.iter().map(String::as_str).collect::<Vec<_>>();
        let tags_all = params
            .tags_all
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let tags_any = params
            .tags_any
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        sqlite::list(
            self.connection,
            &sqlite::ListJobs {
                exclude_running,
                ids: &params.ids,
                keyset: params.keyset(),
                kinds: &kinds,
                limit: i32::try_from(params.limit).unwrap_or(i32::MAX),
                metadata: params.metadata.as_ref(),
                priorities: &params.priorities,
                queues: &queues,
                states: &params.states,
                tags_all: &tags_all,
                tags_any: &tags_any,
            },
        )
        .await
        .map_err(database_error)
    }
}

impl Backend for SqliteBackend<'_> {
    fn connection(&mut self) -> DatabaseConnection<'_> {
        DatabaseConnection::Sqlite(self.connection)
    }

    async fn job_cancel(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let updated = sqlite::cancel(self.connection, id, Utc::now())
            .await
            .map_err(database_error)?;
        let Some(row) = updated else {
            // Finalized jobs are returned unchanged, without a notification.
            return self.job_get(id).await;
        };
        let payload = serde_json::json!({
            "action": "cancel",
            "job_id": id,
            "queue": row.queue,
        })
        .to_string();
        self.notify(crate::NOTIFICATION_TOPIC_CONTROL, &payload)
            .await?;
        Ok(Some(row))
    }

    async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let Some(job) = sqlite::get(self.connection, id)
            .await
            .map_err(database_error)?
        else {
            return Err(Error::NotFound);
        };
        if job.state != JobState::Running {
            return Err(super::job_not_running(job.state.as_str()));
        }
        let now = Utc::now();
        sqlite::complete(
            self.connection,
            &sqlite::CompleteJob {
                attempt: None,
                error: None,
                finalized_at: Some(now),
                id,
                metadata_updates: Some(metadata_updates),
                now,
                scheduled_at: None,
                state: JobState::Completed,
            },
        )
        .await
        .map_err(database_error)?
        .ok_or(Error::NotFound)
    }

    async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error> {
        if let Some(row) = sqlite::delete(self.connection, id)
            .await
            .map_err(database_error)?
        {
            return Ok(row);
        }
        match sqlite::get(self.connection, id)
            .await
            .map_err(database_error)?
        {
            Some(job) if job.state == JobState::Running => Err(Error::JobRunning),
            None | Some(_) => Err(Error::NotFound),
        }
    }

    async fn job_delete_many(&mut self, filter: &JobListParams) -> Result<Vec<JobRow>, Error> {
        // Running jobs are excluded before the limit applies, like Go.
        // SQLite's single writer makes the enclosing write transaction the
        // lock.
        let jobs = self.list(filter, true).await?;
        let mut deleted = Vec::with_capacity(jobs.len());
        for job in jobs {
            if let Some(row) = sqlite::delete(self.connection, job.id)
                .await
                .map_err(database_error)?
            {
                deleted.push(row);
            }
        }
        Ok(deleted)
    }

    async fn job_get(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        sqlite::get(self.connection, id)
            .await
            .map_err(database_error)
    }

    async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        self.list(params, false).await
    }

    async fn job_retry(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let updated = sqlite::retry(self.connection, id, Utc::now())
            .await
            .map_err(database_error)?;
        let Some(row) = updated else {
            // Running and already-available jobs are returned unchanged.
            return self.job_get(id).await;
        };
        let payload = serde_json::json!({"queue": row.queue}).to_string();
        self.notify(crate::NOTIFICATION_TOPIC_INSERT, &payload)
            .await?;
        Ok(Some(row))
    }

    async fn job_update(
        &mut self,
        id: i64,
        metadata: &Map<String, Value>,
    ) -> Result<Option<JobRow>, Error> {
        sqlite::update(self.connection, id, metadata)
            .await
            .map_err(database_error)
    }

    async fn jobs_claim_filtered(
        &mut self,
        client_id: &str,
        max_attempted_by: i32,
        params: &ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        sqlite::claim_filtered(
            self.connection,
            &sqlite::ClaimFilteredJobs {
                client_id,
                excluded_job_id: params.excluded_job_id,
                kind: &params.kind,
                limit: params.maximum,
                max_attempted_by,
                metadata_matches: &params.metadata_matches,
                metadata_updates: &params.metadata_updates,
                now: Utc::now(),
                queue: &params.queue,
            },
        )
        .await
        .map_err(database_error)
    }

    async fn notify(&mut self, topic: &str, payload: &str) -> Result<(), Error> {
        sqlite::notification_insert(
            self.connection,
            &[sqlite::NotificationInput { payload, topic }],
        )
        .await
        .map_err(database_error)?;
        Ok(())
    }

    async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error> {
        sqlite::queue_get(self.connection, name)
            .await
            .map_err(database_error)
    }

    async fn queue_list(&mut self, limit: u32) -> Result<Vec<Queue>, Error> {
        sqlite::queue_list(self.connection, i32::try_from(limit).unwrap_or(i32::MAX))
            .await
            .map_err(database_error)
    }

    async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<u64, Error> {
        let now = Utc::now();
        let updated = if paused {
            sqlite::queue_pause(self.connection, name, now).await
        } else {
            sqlite::queue_resume(self.connection, name, now).await
        }
        .map_err(database_error)?;
        Ok(u64::try_from(updated.len()).unwrap_or(u64::MAX))
    }

    async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error> {
        sqlite::queue_upsert(self.connection, name, &Map::new(), None, Utc::now())
            .await
            .map_err(database_error)
    }

    async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Option<Queue>, Error> {
        let existing;
        let metadata = match metadata {
            Some(metadata) => metadata,
            // Keep the current metadata while refreshing `updated_at`.
            None => match self.queue_get(name).await? {
                Some(queue) => {
                    existing = queue.metadata;
                    &existing
                }
                None => return Ok(None),
            },
        };
        sqlite::queue_update(self.connection, name, metadata, Utc::now())
            .await
            .map_err(database_error)
    }
}

fn database_error(error: sqlite::BackendError) -> Error {
    Error::Database(Box::new(error))
}
