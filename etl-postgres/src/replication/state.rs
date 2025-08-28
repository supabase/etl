use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgExecutor, PgPool, Type, postgres::types::Oid as SqlxTableId, prelude::FromRow};
use tokio_postgres::types::PgLsn;

use crate::types::TableId;

/// Replication state of a table during the ETL process.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableReplicationState {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone {
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
    },
    Ready,
    Errored {
        reason: String,
        solution: Option<String>,
        retry_policy: RetryPolicy,
    },
}

impl TableReplicationState {
    /// Converts state to database storage format with type and metadata.
    ///
    /// Separates the state type from its associated data for efficient querying.
    pub fn to_storage_format(
        &self,
    ) -> Result<(TableReplicationStateType, serde_json::Value), serde_json::Error> {
        let state_type = match self {
            TableReplicationState::Init => TableReplicationStateType::Init,
            TableReplicationState::DataSync => TableReplicationStateType::DataSync,
            TableReplicationState::FinishedCopy => TableReplicationStateType::FinishedCopy,
            TableReplicationState::SyncDone { .. } => TableReplicationStateType::SyncDone,
            TableReplicationState::Ready => TableReplicationStateType::Ready,
            TableReplicationState::Errored { .. } => TableReplicationStateType::Errored,
        };

        let metadata = serde_json::to_value(self)?;

        Ok((state_type, metadata))
    }

    /// Returns whether this state supports manual retry operations.
    ///
    /// Only errored states with [`RetryPolicy::ManualRetry`] can be manually retried.
    pub fn supports_manual_retry(&self) -> bool {
        matches!(
            self,
            TableReplicationState::Errored {
                retry_policy: RetryPolicy::ManualRetry,
                ..
            }
        )
    }
}

/// Retry policy for handling table replication errors.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RetryPolicy {
    NoRetry,
    ManualRetry,
    TimedRetry { next_retry: DateTime<Utc> },
}

/// Database enum type for table replication states.
#[derive(Debug, Clone, Copy, Type, PartialEq)]
#[sqlx(type_name = "etl.table_state", rename_all = "snake_case")]
pub enum TableReplicationStateType {
    Init,
    DataSync,
    FinishedCopy,
    SyncDone,
    Ready,
    Errored,
}

/// Database row representation of table replication state.
#[derive(Debug, FromRow)]
pub struct TableReplicationStateRow {
    pub id: i64,
    pub pipeline_id: i64,
    pub table_id: SqlxTableId,
    pub state: TableReplicationStateType,
    pub metadata: Option<serde_json::Value>,
    pub prev: Option<i64>,
    pub is_current: bool,
}

impl TableReplicationStateRow {
    /// Deserializes metadata field into a [`TableReplicationState`].
    ///
    /// Performs lazy deserialization of the metadata field when the full state
    /// information is needed.
    pub fn deserialize_metadata(&self) -> Result<Option<TableReplicationState>, serde_json::Error> {
        let Some(metadata) = &self.metadata else {
            return Ok(None);
        };

        // Try to deserialize the full state from metadata
        serde_json::from_value(metadata.clone())
    }

    /// Returns the state type without deserializing metadata.
    pub fn state_type(&self) -> TableReplicationStateType {
        self.state
    }
}

/// Fetches current replication state rows for a pipeline.
///
/// Retrieves the current replication state records of all tables of a pipeline.
pub async fn get_table_replication_state_rows(
    pool: &PgPool,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableReplicationStateRow>> {
    let states: Vec<TableReplicationStateRow> = sqlx::query_as(
        r#"
        select id, pipeline_id, table_id, state, metadata, prev, is_current
        from etl.replication_state
        where pipeline_id = $1 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    Ok(states)
}

/// Updates table replication state with history tracking.
///
/// Creates a new state record and chains it to the previous state for audit history,
/// using a transaction to ensure consistency.
pub async fn update_replication_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationState,
) -> sqlx::Result<()> {
    let (state_type, metadata) = state
        .to_storage_format()
        .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    update_replication_state_raw(pool, pipeline_id, table_id, state_type, metadata).await
}

/// Updates replication state using raw database types.
///
/// Internal function that performs the actual database update with pre-serialized
/// state data and proper history chaining.
pub async fn update_replication_state_raw(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
    state: TableReplicationStateType,
    metadata: serde_json::Value,
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;

    // Get the current row's id (if any)
    let current_id: Option<i64> = sqlx::query_scalar(
        r#"
        select id from etl.replication_state 
        where pipeline_id = $1 and table_id = $2 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(&mut *tx)
    .await?;

    // Set current row to not current
    if let Some(prev_id) = current_id {
        sqlx::query(
            r#"
            update etl.replication_state 
            set is_current = false, updated_at = now()
            where id = $1
            "#,
        )
        .bind(prev_id)
        .execute(&mut *tx)
        .await?;
    }

    // Insert new row as current, linking to previous
    sqlx::query(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3, $4, $5, true)
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(state)
    .bind(metadata)
    .bind(current_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(())
}

/// Rolls back table state to the previous entry in the history chain.
///
/// Restores the previous state by updating the current flags in the database,
/// returning the restored state if successful.
pub async fn rollback_replication_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<Option<TableReplicationStateRow>> {
    let mut tx = pool.begin().await?;

    // Get current row and its prev id
    let current_row: Option<(i64, Option<i64>)> = sqlx::query_as(
        r#"
        select id, prev from etl.replication_state 
        where pipeline_id = $1 and table_id = $2 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_optional(&mut *tx)
    .await?;

    if let Some((current_id, Some(prev_id))) = current_row {
        // Set current row to not current
        sqlx::query(
            r#"
            update etl.replication_state
            set is_current = false, updated_at = now()
            where id = $1
            "#,
        )
        .bind(current_id)
        .execute(&mut *tx)
        .await?;

        // Set previous row to current
        sqlx::query(
            r#"
            update etl.replication_state
            set is_current = true, updated_at = now()
            where id = $1
            "#,
        )
        .bind(prev_id)
        .execute(&mut *tx)
        .await?;

        // Fetch the restored row
        let restored_row: TableReplicationStateRow = sqlx::query_as(
            r#"
            select id, pipeline_id, table_id, state, metadata, prev, is_current
            from etl.replication_state
            where id = $1
            "#,
        )
        .bind(prev_id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        return Ok(Some(restored_row));
    }

    tx.rollback().await?;

    Ok(None)
}

/// Resets table replication state to initial state.
///
/// Removes all existing state entries for the table and creates a new
/// [`TableReplicationState::Init`] entry, effectively restarting replication.
pub async fn reset_replication_state(
    pool: &PgPool,
    pipeline_id: i64,
    table_id: TableId,
) -> sqlx::Result<TableReplicationStateRow> {
    let mut tx = pool.begin().await?;

    // Delete all existing entries for this pipeline and table
    sqlx::query(
        r#"
        delete from etl.replication_state 
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(&mut *tx)
    .await?;

    // Insert new Init state entry and return it
    let (state_type, metadata) = TableReplicationState::Init
        .to_storage_format()
        .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    let row: TableReplicationStateRow = sqlx::query_as(
        r#"
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        values ($1, $2, $3, $4, null, true)
        returning id, pipeline_id, table_id, state, metadata, prev, is_current
        "#,
    )
    .bind(pipeline_id)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(state_type)
    .bind(metadata)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(row)
}

/// Deletes all replication state entries for a pipeline.
///
/// Removes all replication state records including historical entries
/// for the specified pipeline. Used during pipeline cleanup.
pub async fn delete_pipeline_replication_state<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<u64>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.replication_state 
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Gets all table IDs that have replication state for a given pipeline.
///
/// Returns a vector of table IDs that are currently being replicated for the specified pipeline.
/// This is useful for operations that need to act on all tables in a pipeline, such as
/// cleaning up replication slots.
pub async fn get_pipeline_table_ids<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> sqlx::Result<Vec<TableId>>
where
    E: PgExecutor<'c>,
{
    let ids: Vec<SqlxTableId> = sqlx::query_scalar(
        r#"
        select distinct table_id
        from etl.replication_state
        where pipeline_id = $1 and is_current = true
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(executor)
    .await?;

    Ok(ids.into_iter().map(|oid| TableId::new(oid.0)).collect())
}

/// Serde serialization helpers for Postgres LSN values.
mod lsn_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tokio_postgres::types::PgLsn;

    /// Serializes a [`PgLsn`] as a string.
    pub fn serialize<S>(lsn: &PgLsn, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        lsn.to_string().serialize(serializer)
    }

    /// Deserializes a [`PgLsn`] from a string representation.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<PgLsn, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map_err(|e| serde::de::Error::custom(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokio_postgres::types::PgLsn;

    #[test]
    fn test_retry_policy_serialization() {
        // Test NoRetry
        let no_retry = RetryPolicy::NoRetry;
        let json = serde_json::to_value(&no_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "no_retry"}));

        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, RetryPolicy::NoRetry));

        // Test ManualRetry
        let manual_retry = RetryPolicy::ManualRetry;
        let json = serde_json::to_value(&manual_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "manual_retry"}));

        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, RetryPolicy::ManualRetry));

        // Test TimedRetry
        let timestamp = Utc::now();
        let timed_retry = RetryPolicy::TimedRetry {
            next_retry: timestamp,
        };
        let json = serde_json::to_value(&timed_retry).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "timed_retry",
                "next_retry": timestamp
            })
        );

        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        if let RetryPolicy::TimedRetry { next_retry } = deserialized {
            assert_eq!(next_retry, timestamp);
        } else {
            panic!("Expected TimedRetry variant");
        }
    }

    #[test]
    fn test_table_replication_phase_serialization() {
        // Test Init
        let init = TableReplicationState::Init;
        let json = serde_json::to_value(&init).unwrap();
        assert_eq!(json, serde_json::json!({"type": "init"}));

        let deserialized: TableReplicationState = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, TableReplicationState::Init);

        // Test SyncDone
        let lsn = "0/1000000".parse::<PgLsn>().unwrap();
        let sync_done = TableReplicationState::SyncDone { lsn };
        let json = serde_json::to_value(&sync_done).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "sync_done",
                "lsn": "0/1000000"
            })
        );

        let deserialized: TableReplicationState = serde_json::from_value(json).unwrap();
        if let TableReplicationState::SyncDone {
            lsn: deserialized_lsn,
        } = deserialized
        {
            assert_eq!(deserialized_lsn, lsn);
        } else {
            panic!("Expected SyncDone variant");
        }

        // Test Errored
        let errored = TableReplicationState::Errored {
            reason: "Test error".to_string(),
            solution: Some("Test solution".to_string()),
            retry_policy: RetryPolicy::NoRetry,
        };
        let json = serde_json::to_value(&errored).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "errored",
                "reason": "Test error",
                "solution": "Test solution",
                "retry_policy": {"type": "no_retry"}
            })
        );

        let deserialized: TableReplicationState = serde_json::from_value(json).unwrap();
        if let TableReplicationState::Errored {
            reason,
            solution,
            retry_policy,
        } = deserialized
        {
            assert_eq!(reason, "Test error");
            assert_eq!(solution, Some("Test solution".to_string()));
            assert!(matches!(retry_policy, RetryPolicy::NoRetry));
        } else {
            panic!("Expected Errored variant");
        }
    }
}
