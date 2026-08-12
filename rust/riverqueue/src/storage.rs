//! Public PostgreSQL CRUD operations.

use serde_json::{Map, Value};
use sqlx::{
    AssertSqlSafe, Executor, FromRow, PgConnection, Postgres, postgres::PgQueryResult, types::Json,
};

use crate::{
    Client, Error, JobDeleteManyParams, JobListOrderBy, JobListParams, JobRow, JobState,
    JobUpdateParams, Queue, QueueListParams, SortDirection,
    client::{ClientInner, JobRecord, job_projection},
};
use riverqueue_internal::{CompletionAction, CompletionParams};

impl Client {
    /// Completes a running job inside a caller-managed transaction. If this is
    /// called from its worker, the normal completer observes that the row is no
    /// longer running and leaves the transactional result unchanged.
    pub async fn job_complete_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
    ) -> Result<JobRow, Error> {
        self.job_complete_tx_with_metadata(connection, id, Map::new())
            .await
    }

    pub(crate) async fn job_complete_tx_with_metadata(
        &self,
        connection: &mut PgConnection,
        id: i64,
        metadata_updates: Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let table = self.inner.schema.qualify("river_job");
        let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
        )))
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?;
        match state.as_deref() {
            None => return Err(Error::NotFound),
            Some("running") => {}
            Some(state) => {
                return Err(Error::InvalidJob(format!(
                    "job must be running for transactional completion; state is {state}"
                )));
            }
        }
        let sql = format!(
            "UPDATE {table} AS job SET state = 'completed', finalized_at = now(), \
             metadata = metadata || $2::jsonb \
             WHERE id = $1 AND state = 'running' \
             RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let completion_action = if self.inner.pilot.intercepts_completion() {
            self.inner
                .pilot
                .before_job_completion(
                    &mut *connection,
                    &CompletionParams {
                        job_id: id,
                        metadata_updates: metadata_updates.clone(),
                        schema: self.inner.schema.clone(),
                        state: JobState::Completed.as_str().to_owned(),
                    },
                )
                .await
                .map_err(|error| Error::Runtime(format!("pilot job completion: {error}")))?
        } else {
            CompletionAction::Continue
        };
        let record = match completion_action {
            CompletionAction::Continue => {
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                    .bind(id)
                    .bind(Json(metadata_updates))
                    .fetch_optional(&mut *connection)
                    .await?
            }
            CompletionAction::Handled => {
                let sql = format!(
                    "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
                     WHERE id = $1 LIMIT 1",
                    job_projection("job")
                );
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(&mut *connection)
                    .await?
            }
        };
        if let Some(record) = record {
            return record.into_job_row();
        }
        Err(Error::NotFound)
    }

    /// Deletes a non-running job and returns its former row.
    pub async fn job_delete(&self, id: i64) -> Result<JobRow, Error> {
        let mut transaction = self.inner.pool.begin().await?;
        let row = job_delete_on(self, &mut transaction, id).await?;
        transaction.commit().await?;
        Ok(row)
    }

    /// Deletes a non-running job inside a caller-managed transaction.
    pub async fn job_delete_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
    ) -> Result<JobRow, Error> {
        job_delete_on(self, connection, id).await
    }

    /// Deletes matching non-running jobs with an explicit safety guard.
    pub async fn job_delete_many(
        &self,
        params: &JobDeleteManyParams,
    ) -> Result<Vec<JobRow>, Error> {
        let mut transaction = self.inner.pool.begin().await?;
        let rows = self.job_delete_many_tx(&mut transaction, params).await?;
        transaction.commit().await?;
        Ok(rows)
    }

    /// Deletes matching non-running jobs inside a caller-managed transaction.
    pub async fn job_delete_many_tx(
        &self,
        connection: &mut PgConnection,
        params: &JobDeleteManyParams,
    ) -> Result<Vec<JobRow>, Error> {
        if !params.all && !params.filter.has_filter() {
            return Err(Error::InvalidJob(
                "bulk delete requires a filter or all=true".to_owned(),
            ));
        }
        let jobs = self.job_list_tx(&mut *connection, &params.filter).await?;
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        let table = self.inner.schema.qualify("river_job");
        let sql = format!(
            "DELETE FROM {table} AS job WHERE id = ANY($1::bigint[]) AND state != 'running' \
             RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(ids)
            .fetch_all(connection)
            .await?;
        let mut rows = records
            .into_iter()
            .map(JobRecord::into_job_row)
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_unstable_by_key(|row| row.id);
        Ok(rows)
    }

    /// Gets one job inside a caller-managed transaction.
    pub async fn job_get_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
    ) -> Result<JobRow, Error> {
        let table = self.inner.schema.qualify("river_job");
        let sql = format!(
            "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job WHERE id = $1 LIMIT 1",
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(connection)
            .await?
            .ok_or(Error::NotFound)?
            .into_job_row()
    }

    /// Lists jobs in ascending ID order.
    pub async fn job_list(&self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        job_list_on(self, &self.inner.pool, params).await
    }

    /// Lists jobs inside a caller-managed transaction.
    pub async fn job_list_tx(
        &self,
        connection: &mut PgConnection,
        params: &JobListParams,
    ) -> Result<Vec<JobRow>, Error> {
        job_list_on(self, connection, params).await
    }

    /// Makes a non-running job immediately available for another attempt.
    pub async fn job_retry(&self, id: i64) -> Result<JobRow, Error> {
        let mut transaction = self.inner.pool.begin().await?;
        let row = job_retry_on(self, &mut transaction, id).await?;
        transaction.commit().await?;
        Ok(row)
    }

    /// Retries a job inside a caller-managed transaction.
    pub async fn job_retry_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
    ) -> Result<JobRow, Error> {
        job_retry_on(self, connection, id).await
    }

    /// Merges job metadata and optionally sets recorded output.
    pub async fn job_update(&self, id: i64, params: JobUpdateParams) -> Result<JobRow, Error> {
        job_update_on(self, &self.inner.pool, id, params).await
    }

    /// Updates a job inside a caller-managed transaction.
    pub async fn job_update_tx(
        &self,
        connection: &mut PgConnection,
        id: i64,
        params: JobUpdateParams,
    ) -> Result<JobRow, Error> {
        job_update_on(self, connection, id, params).await
    }

    /// Gets one active queue record.
    pub async fn queue_get(&self, name: &str) -> Result<Queue, Error> {
        queue_get_on(self, &self.inner.pool, name).await
    }

    /// Gets one queue inside a caller-managed transaction.
    pub async fn queue_get_tx(
        &self,
        connection: &mut PgConnection,
        name: &str,
    ) -> Result<Queue, Error> {
        queue_get_on(self, connection, name).await
    }

    /// Lists active queues by name.
    pub async fn queue_list(&self, params: &QueueListParams) -> Result<Vec<Queue>, Error> {
        queue_list_on(self, &self.inner.pool, params).await
    }

    /// Lists queues inside a caller-managed transaction.
    pub async fn queue_list_tx(
        &self,
        connection: &mut PgConnection,
        params: &QueueListParams,
    ) -> Result<Vec<Queue>, Error> {
        queue_list_on(self, connection, params).await
    }

    /// Pauses one queue, or every known queue when passed `"*"`.
    pub async fn queue_pause(&self, name: &str) -> Result<(), Error> {
        let mut transaction = self.inner.pool.begin().await?;
        queue_set_paused_on(self, &mut transaction, name, true).await?;
        transaction.commit().await?;
        self.emit_queue_events(name, true).await?;
        Ok(())
    }

    /// Pauses queues inside a caller-managed transaction.
    pub async fn queue_pause_tx(
        &self,
        connection: &mut PgConnection,
        name: &str,
    ) -> Result<(), Error> {
        queue_set_paused_on(self, connection, name, true).await
    }

    /// Resumes one queue, or every known queue when passed `"*"`.
    pub async fn queue_resume(&self, name: &str) -> Result<(), Error> {
        let mut transaction = self.inner.pool.begin().await?;
        queue_set_paused_on(self, &mut transaction, name, false).await?;
        transaction.commit().await?;
        self.emit_queue_events(name, false).await?;
        Ok(())
    }

    /// Resumes queues inside a caller-managed transaction.
    pub async fn queue_resume_tx(
        &self,
        connection: &mut PgConnection,
        name: &str,
    ) -> Result<(), Error> {
        queue_set_paused_on(self, connection, name, false).await
    }

    /// Replaces a queue's metadata object.
    pub async fn queue_update(
        &self,
        name: &str,
        metadata: Map<String, Value>,
    ) -> Result<Queue, Error> {
        let mut transaction = self.inner.pool.begin().await?;
        let queue = queue_update_on(self, &mut transaction, name, metadata).await?;
        transaction.commit().await?;
        Ok(queue)
    }

    /// Updates queue metadata inside a caller-managed transaction.
    pub async fn queue_update_tx(
        &self,
        connection: &mut PgConnection,
        name: &str,
        metadata: Map<String, Value>,
    ) -> Result<Queue, Error> {
        queue_update_on(self, connection, name, metadata).await
    }

    async fn emit_queue_events(&self, name: &str, paused: bool) -> Result<(), Error> {
        let queues = if name == "*" {
            self.queue_list(&QueueListParams { limit: 10_000 }).await?
        } else {
            vec![self.queue_get(name).await?]
        };
        let kind = if paused {
            crate::EventKind::QueuePaused
        } else {
            crate::EventKind::QueueResumed
        };
        for queue in queues {
            let _ = self.inner.events.send(crate::Event::queue(kind, queue));
        }
        Ok(())
    }
}

async fn job_delete_on(
    client: &Client,
    connection: &mut PgConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let table = client.inner.schema.qualify("river_job");
    let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
    )))
    .bind(id)
    .fetch_optional(&mut *connection)
    .await?;
    match state.as_deref() {
        None => return Err(Error::NotFound),
        Some("running") => return Err(Error::JobRunning),
        Some(_) => {}
    }
    let sql = format!(
        "DELETE FROM {table} AS job WHERE id = $1 RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .fetch_one(connection)
        .await?
        .into_job_row()
}

async fn job_list_on<'executor, E>(
    client: &Client,
    executor: E,
    params: &JobListParams,
) -> Result<Vec<JobRow>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    params.validate().map_err(Error::InvalidJob)?;
    let table = client.inner.schema.qualify("river_job");
    let sort_field = match params.order_by {
        JobListOrderBy::FinalizedAt => "finalized_at",
        JobListOrderBy::Id => "id",
        JobListOrderBy::ScheduledAt => "scheduled_at",
        JobListOrderBy::Time => match params
            .states
            .first()
            .copied()
            .unwrap_or(JobState::Available)
        {
            JobState::Available | JobState::Pending | JobState::Retryable | JobState::Scheduled => {
                "scheduled_at"
            }
            JobState::Running => "attempted_at",
            JobState::Cancelled | JobState::Completed | JobState::Discarded => "finalized_at",
        },
    };
    let direction = match params.direction {
        SortDirection::Ascending => "ASC",
        SortDirection::Descending => "DESC",
    };
    let comparison = match params.direction {
        SortDirection::Ascending => ">",
        SortDirection::Descending => "<",
    };
    let cursor_id = params
        .after
        .as_ref()
        .map(|cursor| cursor.id)
        .or(params.after_id);
    let cursor_time = params.after.as_ref().and_then(|cursor| cursor.sort_time);
    let cursor_predicate = if params.order_by == JobListOrderBy::Id {
        format!("($10::bigint IS NULL OR id {comparison} $10)")
    } else {
        format!(
            "($9::timestamptz IS NULL OR ({sort_field} {comparison} $9 OR \
             ({sort_field} = $9 AND id {comparison} $10)))"
        )
    };
    let order = if params.order_by == JobListOrderBy::Id {
        format!("id {direction}")
    } else {
        format!("{sort_field} {direction}, id {direction}")
    };
    let sql = format!(
        "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
         WHERE (cardinality($1::bigint[]) = 0 OR id = ANY($1)) \
           AND (cardinality($2::text[]) = 0 OR kind = ANY($2)) \
           AND (cardinality($3::text[]) = 0 OR queue = ANY($3)) \
           AND (cardinality($4::text[]) = 0 OR state::text = ANY($4)) \
           AND (cardinality($5::smallint[]) = 0 OR priority = ANY($5)) \
           AND (cardinality($6::varchar[]) = 0 OR tags @> $6::varchar[]) \
           AND (cardinality($7::varchar[]) = 0 OR tags && $7::varchar[]) \
           AND ($8::jsonb IS NULL OR metadata @> $8) \
           AND {cursor_predicate} \
         ORDER BY {order} LIMIT $11",
        job_projection("job")
    );
    let states = params
        .states
        .iter()
        .map(|state| state.as_str().to_owned())
        .collect::<Vec<_>>();
    let metadata = params
        .metadata
        .as_ref()
        .map(|metadata| Json(Value::Object(metadata.clone())));
    let records = sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(&params.ids)
        .bind(&params.kinds)
        .bind(&params.queues)
        .bind(states)
        .bind(&params.priorities)
        .bind(&params.tags_all)
        .bind(&params.tags_any)
        .bind(metadata)
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(params.limit)
        .fetch_all(executor)
        .await?;
    records.into_iter().map(JobRecord::into_job_row).collect()
}

async fn job_retry_on(
    client: &Client,
    connection: &mut PgConnection,
    id: i64,
) -> Result<JobRow, Error> {
    let table = client.inner.schema.qualify("river_job");
    let sql = format!(
        "WITH locked AS (SELECT id FROM {table} WHERE id = $1 FOR UPDATE), \
         updated AS (UPDATE {table} AS job SET state = 'available', \
             max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END, \
             finalized_at = NULL, scheduled_at = now() \
             FROM locked WHERE job.id = locked.id AND job.state != 'running' \
               AND NOT (job.state = 'available' AND job.scheduled_at < now()) RETURNING job.*), \
         notified AS (SELECT pg_notify(concat(coalesce($2::text, current_schema()), '.', $3::text), \
             json_build_object('queue', queue)::text) FROM updated WHERE state = 'available') \
         SELECT {}, false AS unique_skipped_as_duplicate FROM updated AS job LEFT JOIN notified ON true \
         UNION ALL SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated) LIMIT 1",
        job_projection("job"),
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .bind(client.inner.schema.as_deref())
        .bind(crate::NOTIFICATION_TOPIC_INSERT)
        .fetch_optional(connection)
        .await?
        .ok_or(Error::NotFound)?
        .into_job_row()
}

async fn job_update_on<'executor, E>(
    client: &Client,
    executor: E,
    id: i64,
    params: JobUpdateParams,
) -> Result<JobRow, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    let mut metadata = params.metadata;
    if let Some(output) = params.output {
        metadata.insert(crate::METADATA_KEY_OUTPUT.to_owned(), output);
    }
    let table = client.inner.schema.qualify("river_job");
    let sql = format!(
        "UPDATE {table} AS job SET metadata = metadata || $2::jsonb WHERE id = $1 \
         RETURNING {}, false AS unique_skipped_as_duplicate",
        job_projection("job")
    );
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(id)
        .bind(Json(metadata))
        .fetch_optional(executor)
        .await?
        .ok_or(Error::NotFound)?
        .into_job_row()
}

#[derive(FromRow)]
struct QueueRecord {
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: Json<Value>,
    name: String,
    paused_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl QueueRecord {
    fn into_queue(self) -> Result<Queue, Error> {
        Ok(Queue {
            created_at: self.created_at,
            metadata: self.metadata.0.as_object().cloned().ok_or_else(|| {
                Error::InvalidJob(format!("queue {:?} metadata is not an object", self.name))
            })?,
            name: self.name,
            paused_at: self.paused_at,
            updated_at: self.updated_at,
        })
    }
}

async fn queue_get_on<'executor, E>(
    client: &Client,
    executor: E,
    name: &str,
) -> Result<Queue, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    let table = client.inner.schema.qualify("river_queue");
    let sql = format!("SELECT * FROM {table} WHERE name = $1");
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(name)
        .fetch_optional(executor)
        .await?
        .ok_or(Error::NotFound)?
        .into_queue()
}

async fn queue_list_on<'executor, E>(
    client: &Client,
    executor: E,
    params: &QueueListParams,
) -> Result<Vec<Queue>, Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    if !(1..=10_000).contains(&params.limit) {
        return Err(Error::InvalidJob(
            "queue list limit must be between 1 and 10000".to_owned(),
        ));
    }
    let table = client.inner.schema.qualify("river_queue");
    let sql = format!("SELECT * FROM {table} ORDER BY name LIMIT $1");
    sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(params.limit)
        .fetch_all(executor)
        .await?
        .into_iter()
        .map(QueueRecord::into_queue)
        .collect()
}

async fn queue_set_paused_on(
    client: &Client,
    connection: &mut PgConnection,
    name: &str,
    paused: bool,
) -> Result<(), Error> {
    if name != "*" {
        super::client::validate_queue(name)?;
    }
    let table = client.inner.schema.qualify("river_queue");
    let sql = if paused {
        format!(
            "UPDATE {table} SET paused_at = coalesce(paused_at, now()), \
             updated_at = CASE WHEN paused_at IS NULL THEN now() ELSE updated_at END \
             WHERE $1 = '*' OR name = $1"
        )
    } else {
        format!(
            "UPDATE {table} SET updated_at = CASE WHEN paused_at IS NOT NULL THEN now() ELSE updated_at END, \
             paused_at = NULL WHERE $1 = '*' OR name = $1"
        )
    };
    sqlx::query(AssertSqlSafe(sql))
        .bind(name)
        .execute(&mut *connection)
        .await?;
    notify_control(
        client,
        connection,
        serde_json::json!({"action": if paused {"pause"} else {"resume"}, "queue": name}),
    )
    .await
}

async fn queue_update_on(
    client: &Client,
    connection: &mut PgConnection,
    name: &str,
    metadata: Map<String, Value>,
) -> Result<Queue, Error> {
    super::client::validate_queue(name)?;
    let table = client.inner.schema.qualify("river_queue");
    let sql =
        format!("UPDATE {table} SET metadata = $2, updated_at = now() WHERE name = $1 RETURNING *");
    let queue = sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
        .bind(name)
        .bind(Json(metadata.clone()))
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(Error::NotFound)?
        .into_queue()?;
    notify_control(
        client,
        connection,
        serde_json::json!({"action": "metadata_changed", "metadata": metadata, "queue": name}),
    )
    .await?;
    Ok(queue)
}

async fn notify_control(
    client: &Client,
    connection: &mut PgConnection,
    payload: Value,
) -> Result<(), Error> {
    sqlx::query(
        "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), $3)",
    )
    .bind(client.inner.schema.as_deref())
    .bind(crate::NOTIFICATION_TOPIC_CONTROL)
    .bind(payload.to_string())
    .execute(connection)
    .await?;
    Ok(())
}

pub(crate) async fn touch_queue(inner: &ClientInner, name: &str) -> Result<PgQueryResult, Error> {
    let table = inner.schema.qualify("river_queue");
    let sql = format!(
        "INSERT INTO {table} (name, metadata, updated_at) VALUES ($1, '{{}}'::jsonb, now()) \
         ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at"
    );
    Ok(sqlx::query(AssertSqlSafe(sql))
        .bind(name)
        .execute(&inner.pool)
        .await?)
}
