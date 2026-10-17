//! MySQL implementation of River's storage operations.

use chrono::Utc;
use serde_json::{Map, Value};
use sqlx::MySqlConnection;

use super::Backend;
use crate::__private::DatabaseConnection;
use crate::database::mysql;
use crate::{Error, JobListParams, JobRow, JobState, Queue};

/// MySQL storage bound to one connection.
pub(super) struct MySqlBackend<'c> {
    pub(super) connection: &'c mut MySqlConnection,
}

impl MySqlBackend<'_> {
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
        mysql::list(
            self.connection,
            &mysql::ListJobs {
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

impl Backend for MySqlBackend<'_> {
    fn connection(&mut self) -> DatabaseConnection<'_> {
        DatabaseConnection::MySql(self.connection)
    }

    async fn job_cancel(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let updated = mysql::cancel(self.connection, id, Utc::now())
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

    // Like River Go, metadata that isn't valid JSON doesn't fail the lookup
    // for the other jobs.
    async fn job_cancel_requested(&mut self, ids: &[i64]) -> Result<Vec<i64>, Error> {
        let ids = format!(
            "[{}]",
            ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
        );
        Ok(sqlx::query_scalar(
            "SELECT id FROM river_job \
             WHERE id IN (SELECT id FROM JSON_TABLE(?, '$[*]' COLUMNS(id BIGINT PATH '$')) AS wanted) \
               AND JSON_CONTAINS_PATH(metadata, 'one', '$.cancel_attempted_at') \
               AND state = 'running' \
             ORDER BY id",
        )
        .bind(ids)
        .fetch_all(&mut *self.connection)
        .await?)
    }

    async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let Some(job) = mysql::get_locked(self.connection, id)
            .await
            .map_err(database_error)?
        else {
            return Err(Error::NotFound(crate::Record::Job(id)));
        };
        if job.state != JobState::Running {
            return Err(super::job_not_running(job.state));
        }
        let now = Utc::now();
        mysql::complete(
            self.connection,
            &mysql::CompleteJob {
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
        .ok_or(Error::NotFound(crate::Record::Job(id)))
    }

    async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error> {
        if let Some(row) = mysql::delete(self.connection, id)
            .await
            .map_err(database_error)?
        {
            return Ok(row);
        }
        match mysql::get(self.connection, id)
            .await
            .map_err(database_error)?
        {
            Some(job) if job.state == JobState::Running => Err(Error::JobRunning),
            None | Some(_) => Err(Error::NotFound(crate::Record::Job(id))),
        }
    }

    async fn job_delete_many(&mut self, filter: &JobListParams) -> Result<Vec<JobRow>, Error> {
        // Running jobs are excluded before the limit applies, like Go.
        // Each delete locks and rechecks the candidate's state.
        let jobs = self.list(filter, true).await?;
        let mut deleted = Vec::with_capacity(jobs.len());
        for job in jobs {
            if let Some(row) = mysql::delete(self.connection, job.id)
                .await
                .map_err(database_error)?
            {
                deleted.push(row);
            }
        }
        Ok(deleted)
    }

    async fn job_claim(
        &mut self,
        id: i64,
        client_id: &str,
        max_attempted_by: i32,
    ) -> Result<Option<JobRow>, Error> {
        let claimed = mysql::claim_selected(
            self.connection,
            &mysql::ClaimJobs {
                client_id,
                kinds: None,
                limit: 1,
                max_attempted_by,
                now: Utc::now(),
                queue: "",
            },
            &[id],
        )
        .await
        .map_err(database_error)?;
        Ok(claimed
            .into_iter()
            .next()
            .and_then(crate::client::tolerant_row))
    }

    async fn job_get(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        mysql::get(self.connection, id)
            .await
            .map_err(database_error)
    }

    async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        self.list(params, false).await
    }

    async fn job_retry(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let updated = mysql::retry(self.connection, id, Utc::now())
            .await
            .map_err(database_error)?;
        let Some(row) = updated else {
            // Running and already-available jobs are returned unchanged.
            return self.job_get(id).await;
        };
        // Like Go's `JobRetry`, a retry sends no insert notification;
        // producers find the job on their next poll.
        Ok(Some(row))
    }

    async fn job_update(
        &mut self,
        id: i64,
        metadata: &Map<String, Value>,
    ) -> Result<Option<JobRow>, Error> {
        mysql::update(self.connection, id, metadata)
            .await
            .map_err(database_error)
    }

    async fn notify(&mut self, topic: &str, payload: &str) -> Result<(), Error> {
        mysql::notification_insert(
            self.connection,
            &[mysql::NotificationInput { payload, topic }],
        )
        .await
        .map_err(database_error)?;
        Ok(())
    }

    async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error> {
        mysql::queue_get(self.connection, name)
            .await
            .map_err(database_error)
    }

    async fn queue_list(&mut self, limit: u32) -> Result<Vec<Queue>, Error> {
        mysql::queue_list(self.connection, i32::try_from(limit).unwrap_or(i32::MAX))
            .await
            .map_err(database_error)
    }

    async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<u64, Error> {
        let now = Utc::now();
        let updated = if paused {
            mysql::queue_pause(self.connection, name, now).await
        } else {
            mysql::queue_resume(self.connection, name, now).await
        }
        .map_err(database_error)?;
        Ok(u64::try_from(updated.len()).unwrap_or(u64::MAX))
    }

    async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error> {
        mysql::queue_upsert(self.connection, name, &Map::new(), None, Utc::now())
            .await
            .map_err(database_error)
    }

    async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Option<Queue>, Error> {
        mysql::queue_update(self.connection, name, metadata, Utc::now())
            .await
            .map_err(database_error)
    }
}

fn database_error(error: mysql::BackendError) -> Error {
    Error::Database(error.into())
}
