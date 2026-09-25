//! PostgreSQL implementation of River's storage operations.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use sqlx::{AssertSqlSafe, FromRow, PgConnection, Postgres, types::Json};

use super::Backend;
use crate::__private::{DatabaseConnection, ExtensionClaimParams};
use crate::client::{JobRecord, go_time_json, job_projection};
use crate::{
    Error, JobListOrderBy, JobListParams, JobRow, JobState, Queue, SchemaName, SortDirection,
};

/// PostgreSQL storage bound to one connection.
pub(super) struct PostgresBackend<'c> {
    pub(super) connection: &'c mut PgConnection,
    pub(super) schema: &'c SchemaName,
}

impl Backend for PostgresBackend<'_> {
    fn connection(&mut self) -> DatabaseConnection<'_> {
        DatabaseConnection::Postgres(self.connection)
    }

    async fn job_cancel(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let sql = format!(
            "WITH locked AS (\
                SELECT id, queue, state, finalized_at FROM {table} WHERE id = $1 FOR UPDATE\
             ), notified AS (\
                SELECT id, pg_notify(concat(coalesce($2::text, current_schema()), '.', $3::text), json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)\
                FROM locked WHERE state NOT IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NULL\
             ), updated AS (\
                UPDATE {table} AS job SET \
                    state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END, \
                    finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE now() END, \
                    metadata = jsonb_set(metadata, '{{cancel_attempted_at}}'::text[], to_jsonb($4::text), true) \
                FROM notified WHERE job.id = notified.id RETURNING job.*\
             ) \
             SELECT {}, false AS unique_skipped_as_duplicate FROM updated AS job \
             UNION ALL \
             SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated) LIMIT 1",
            job_projection("job"),
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .bind(self.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_CONTROL)
            .bind(go_time_json(Utc::now()))
            .fetch_optional(&mut *self.connection)
            .await?
            .map(JobRecord::into_job_row)
            .transpose()
    }

    async fn job_complete(
        &mut self,
        id: i64,
        metadata_updates: &Map<String, Value>,
    ) -> Result<JobRow, Error> {
        let table = self.schema.qualify("river_job");
        let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
        )))
        .bind(id)
        .fetch_optional(&mut *self.connection)
        .await?;
        match state.as_deref() {
            None => return Err(Error::NotFound),
            Some("running") => {}
            Some(state) => return Err(super::job_not_running(state)),
        }
        let sql = format!(
            "UPDATE {table} AS job SET state = 'completed', finalized_at = now(), \
             metadata = metadata || $2::jsonb \
             WHERE id = $1 AND state = 'running' \
             RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .bind(Json(metadata_updates))
            .fetch_optional(&mut *self.connection)
            .await?
            .ok_or(Error::NotFound)?
            .into_job_row()
    }

    async fn job_delete(&mut self, id: i64) -> Result<JobRow, Error> {
        let table = self.schema.qualify("river_job");
        let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {table} WHERE id = $1 FOR UPDATE"
        )))
        .bind(id)
        .fetch_optional(&mut *self.connection)
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
            .fetch_one(&mut *self.connection)
            .await?
            .into_job_row()
    }

    async fn job_delete_many(&mut self, filter: &JobListParams) -> Result<Vec<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let parts = job_list_sql_parts(self.schema, filter, false);
        // Mirrors Go's `JobDeleteMany`: running jobs are excluded before the
        // limit applies, candidates already locked by another transaction are
        // skipped rather than waited on, and rows come back in the list order.
        let sql = format!(
            "WITH jobs_to_delete AS (\
                SELECT id FROM {table} AS job WHERE {where_sql} AND state != 'running' \
                ORDER BY {order_sql} LIMIT $11 FOR UPDATE SKIP LOCKED\
             ), deleted AS (\
                DELETE FROM {table} WHERE id IN (SELECT id FROM jobs_to_delete) RETURNING *\
             ) \
             SELECT {}, false AS unique_skipped_as_duplicate FROM deleted AS job ORDER BY {order_sql}",
            job_projection("job"),
            where_sql = parts.where_sql,
            order_sql = parts.order_sql,
        );
        let records = bind_job_list(sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql)), filter)
            .fetch_all(&mut *self.connection)
            .await?;
        records.into_iter().map(JobRecord::into_job_row).collect()
    }

    async fn job_get(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let sql = format!(
            "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job WHERE id = $1 LIMIT 1",
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut *self.connection)
            .await?
            .map(JobRecord::into_job_row)
            .transpose()
    }

    async fn job_list(&mut self, params: &JobListParams) -> Result<Vec<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let parts = job_list_sql_parts(self.schema, params, true);
        let sql = format!(
            "SELECT {}, false AS unique_skipped_as_duplicate FROM {table} AS job \
             WHERE {} ORDER BY {} LIMIT $11",
            job_projection("job"),
            parts.where_sql,
            parts.order_sql,
        );
        let records = bind_job_list(sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql)), params)
            .fetch_all(&mut *self.connection)
            .await?;
        records.into_iter().map(JobRecord::into_job_row).collect()
    }

    async fn job_retry(&mut self, id: i64) -> Result<Option<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
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
            .bind(self.schema.as_deref())
            .bind(crate::NOTIFICATION_TOPIC_INSERT)
            .fetch_optional(&mut *self.connection)
            .await?
            .map(JobRecord::into_job_row)
            .transpose()
    }

    async fn job_update(
        &mut self,
        id: i64,
        metadata: &Map<String, Value>,
    ) -> Result<Option<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let sql = format!(
            "UPDATE {table} AS job SET metadata = metadata || $2::jsonb WHERE id = $1 \
             RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(id)
            .bind(Json(metadata))
            .fetch_optional(&mut *self.connection)
            .await?
            .map(JobRecord::into_job_row)
            .transpose()
    }

    async fn jobs_claim_filtered(
        &mut self,
        client_id: &str,
        max_attempted_by: i32,
        params: &ExtensionClaimParams,
    ) -> Result<Vec<JobRow>, Error> {
        let table = self.schema.qualify("river_job");
        let sql = format!(
            "WITH locked AS (\
                SELECT id FROM {table} \
                WHERE state = 'available' AND queue = $1 AND kind = $2 \
                  AND id != $3 AND scheduled_at <= now() \
                  AND metadata @> $4::jsonb \
                ORDER BY priority ASC, scheduled_at ASC, id ASC \
                LIMIT $5 FOR UPDATE SKIP LOCKED\
             ) UPDATE {table} AS job \
                SET state = 'running', attempt = job.attempt + 1, \
                    attempted_at = now(), attempted_by = array_append(\
                        CASE WHEN array_length(job.attempted_by, 1) >= $7 \
                             THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $7:] \
                             ELSE job.attempted_by END, $6), \
                    metadata = job.metadata || $8::jsonb \
                FROM locked WHERE job.id = locked.id \
                RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
            .bind(&params.queue)
            .bind(&params.kind)
            .bind(params.excluded_job_id)
            .bind(Json(&params.metadata_matches))
            .bind(params.maximum)
            .bind(client_id)
            .bind(max_attempted_by)
            .bind(Json(&params.metadata_updates))
            .fetch_all(&mut *self.connection)
            .await?
            .into_iter()
            .map(JobRecord::into_job_row)
            .collect()
    }

    async fn notify(&mut self, topic: &str, payload: &str) -> Result<(), Error> {
        sqlx::query(
            "SELECT pg_notify(concat(coalesce($1::text, current_schema()), '.', $2::text), $3::text)",
        )
        .bind(self.schema.as_deref())
        .bind(topic)
        .bind(payload)
        .execute(&mut *self.connection)
        .await?;
        Ok(())
    }

    async fn queue_get(&mut self, name: &str) -> Result<Option<Queue>, Error> {
        let table = self.schema.qualify("river_queue");
        sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(format!(
            "SELECT * FROM {table} WHERE name = $1"
        )))
        .bind(name)
        .fetch_optional(&mut *self.connection)
        .await?
        .map(QueueRecord::into_queue)
        .transpose()
    }

    async fn queue_list(&mut self, limit: u32) -> Result<Vec<Queue>, Error> {
        let table = self.schema.qualify("river_queue");
        sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(format!(
            "SELECT * FROM {table} ORDER BY name LIMIT $1"
        )))
        .bind(i64::from(limit))
        .fetch_all(&mut *self.connection)
        .await?
        .into_iter()
        .map(QueueRecord::into_queue)
        .collect()
    }

    async fn queue_set_paused(&mut self, name: &str, paused: bool) -> Result<u64, Error> {
        let table = self.schema.qualify("river_queue");
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
        Ok(sqlx::query(AssertSqlSafe(sql))
            .bind(name)
            .execute(&mut *self.connection)
            .await?
            .rows_affected())
    }

    async fn queue_touch(&mut self, name: &str) -> Result<Queue, Error> {
        let table = self.schema.qualify("river_queue");
        let sql = format!(
            "INSERT INTO {table} (name, metadata, updated_at) VALUES ($1, '{{}}'::jsonb, now()) \
             ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at RETURNING *"
        );
        sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
            .bind(name)
            .fetch_one(&mut *self.connection)
            .await?
            .into_queue()
    }

    async fn queue_update(
        &mut self,
        name: &str,
        metadata: Option<&Map<String, Value>>,
    ) -> Result<Option<Queue>, Error> {
        let table = self.schema.qualify("river_queue");
        let sql = format!(
            "UPDATE {table} SET metadata = CASE WHEN $2::boolean THEN $3::jsonb ELSE metadata END, \
             updated_at = now() WHERE name = $1 RETURNING *"
        );
        sqlx::query_as::<_, QueueRecord>(AssertSqlSafe(sql))
            .bind(name)
            .bind(metadata.is_some())
            .bind(metadata.map(Json))
            .fetch_optional(&mut *self.connection)
            .await?
            .map(QueueRecord::into_queue)
            .transpose()
    }
}

#[derive(FromRow)]
struct QueueRecord {
    created_at: DateTime<Utc>,
    metadata: Json<Value>,
    name: String,
    paused_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

impl QueueRecord {
    fn into_queue(self) -> Result<Queue, Error> {
        Ok(Queue {
            created_at: self.created_at,
            metadata: self.metadata.0.as_object().cloned().ok_or_else(|| {
                Error::invalid_job_context(
                    "storage parameters",
                    format!("queue {:?} metadata is not an object", self.name),
                )
            })?,
            name: self.name,
            paused_at: self.paused_at,
            updated_at: self.updated_at,
        })
    }
}

/// SQL fragments shared by job listing and bulk deletion. Both bind the same
/// eleven positional parameters through [`bind_job_list`].
struct JobListSqlParts {
    order_sql: String,
    where_sql: String,
}

fn job_list_sql_parts(
    schema: &SchemaName,
    params: &JobListParams,
    optimize_single_state: bool,
) -> JobListSqlParts {
    let sort_field = job_list_sort_field(params);
    let direction = match params.direction {
        SortDirection::Ascending => "ASC",
        SortDirection::Descending => "DESC",
    };
    let comparison = match params.direction {
        SortDirection::Ascending => ">",
        SortDirection::Descending => "<",
    };
    let cursor_predicate = if sort_field == "id" {
        format!("($10::bigint IS NULL OR id {comparison} $10)")
    } else {
        format!(
            "($9::timestamptz IS NULL OR ({sort_field} {comparison} $9 OR \
             ({sort_field} = $9 AND id {comparison} $10)))"
        )
    };
    let state_type = schema.qualify("river_job_state");
    // Like Go (upstream 35c4eab8), a single-state list without metadata
    // predicates compares state with equality so PostgreSQL can use the
    // `(state, <time>)` index ordering, and a single finalized state ordered by
    // `finalized_at` states the non-null invariant that the partial
    // finalized-time index requires. Bulk deletion keeps the generic form.
    let state_predicate =
        if optimize_single_state && params.states.len() == 1 && params.metadata.is_none() {
            let finalized = sort_field == "finalized_at"
                && matches!(
                    params.states[0],
                    JobState::Cancelled | JobState::Completed | JobState::Discarded
                );
            format!(
                "state = ($4::text[])[1]::{state_type}{}",
                if finalized {
                    " AND finalized_at IS NOT NULL"
                } else {
                    ""
                }
            )
        } else {
            format!("(cardinality($4::text[]) = 0 OR state = ANY($4::text[]::{state_type}[]))")
        };
    let where_sql = format!(
        "(cardinality($1::bigint[]) = 0 OR id = ANY($1)) \
         AND (cardinality($2::text[]) = 0 OR kind = ANY($2)) \
         AND (cardinality($3::text[]) = 0 OR queue = ANY($3)) \
         AND {state_predicate} \
         AND (cardinality($5::smallint[]) = 0 OR priority = ANY($5)) \
         AND (cardinality($6::varchar[]) = 0 OR tags @> $6::varchar[]) \
         AND (cardinality($7::varchar[]) = 0 OR tags && $7::varchar[]) \
         AND ($8::jsonb IS NULL OR metadata @> $8) \
         AND {cursor_predicate}"
    );
    let order_sql = if sort_field == "id" {
        format!("id {direction}")
    } else {
        format!("{sort_field} {direction}, id {direction}")
    };
    JobListSqlParts {
        order_sql,
        where_sql,
    }
}

fn job_list_sort_field(params: &JobListParams) -> &'static str {
    match params.order_by {
        JobListOrderBy::FinalizedAt => "finalized_at",
        JobListOrderBy::Id => "id",
        JobListOrderBy::ScheduledAt => "scheduled_at",
        JobListOrderBy::Time if params.states.is_empty() => "id",
        JobListOrderBy::Time => match params.states[0] {
            JobState::Available | JobState::Pending | JobState::Retryable | JobState::Scheduled => {
                "scheduled_at"
            }
            JobState::Running => "attempted_at",
            JobState::Cancelled | JobState::Completed | JobState::Discarded => "finalized_at",
        },
    }
}

fn bind_job_list<'query>(
    query: sqlx::query::QueryAs<'query, Postgres, JobRecord, sqlx::postgres::PgArguments>,
    params: &'query JobListParams,
) -> sqlx::query::QueryAs<'query, Postgres, JobRecord, sqlx::postgres::PgArguments> {
    let states = params
        .states
        .iter()
        .map(|state| state.as_str().to_owned())
        .collect::<Vec<_>>();
    let metadata = params
        .metadata
        .as_ref()
        .map(|metadata| Json(Value::Object(metadata.clone())));
    query
        .bind(&params.ids)
        .bind(&params.kinds)
        .bind(&params.queues)
        .bind(states)
        .bind(&params.priorities)
        .bind(&params.tags_all)
        .bind(&params.tags_any)
        .bind(metadata)
        .bind(params.cursor_time())
        .bind(params.cursor_id())
        .bind(i64::from(params.limit))
}
