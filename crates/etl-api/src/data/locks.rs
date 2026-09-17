//! Transaction-scoped exclusion for operations on existing pipelines.
//!
//! Acquire the complete pipeline set once, before state changes or Kubernetes
//! work. Locks use `nowait`, so contention fails instead of queuing an obsolete
//! operation. Callers own the API transaction and its commit or rollback
//! boundary.
//!
//! Pipeline IDs are the common exclusion boundary for lifecycle and
//! configuration changes. A source or destination update discovers its existing
//! pipelines, locks that complete set, writes the configuration, and restarts
//! active pipelines before committing. A pipeline-only change locks its target.
//!
//! This is deliberately limited to the discovered pipeline rows. It does not
//! lock source, destination, or tenant rows to freeze membership: concurrent
//! pipeline creation or attachment can be missed, even for an existing
//! pipeline. At the default Read Committed isolation level, each query sees its
//! own snapshot; acquiring the discovered locks does not discover later
//! additions. A missed pipeline can therefore run with the previous
//! configuration until another reconciliation. Operations with no discovered
//! pipelines have no lifecycle exclusion through this module.

use sqlx::PgTransaction;

use crate::routes::pipelines::PipelineError;

/// Excludes overlapping operations on one existing pipeline across API
/// instances.
///
/// Retain the transaction through validation, state changes, and Kubernetes
/// work. Ordinary reads remain unblocked. Tenant scoping preserves 404
/// responses for another tenant's pipeline even when its row is locked.
pub(crate) async fn lock_pipeline(
    api_txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<(), PipelineError> {
    sqlx::query_scalar::<_, i64>(
        "select id from app.pipelines where tenant_id = $1 and id = $2 for update nowait",
    )
    .bind(tenant_id)
    .bind(pipeline_id)
    .fetch_optional(&mut **api_txn)
    .await
    .map_err(|error| {
        if error.as_database_error().is_some_and(|error| error.code().as_deref() == Some("55P03")) {
            PipelineError::OperationInProgress(pipeline_id)
        } else {
            PipelineError::Database(error)
        }
    })?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(())
}

/// Acquires the complete pipeline set in ID order before any side effects.
///
/// On contention, callers must return the error so dropping the transaction
/// rolls back and releases any locks already acquired. Never skip locked rows.
pub(crate) async fn lock_pipelines(
    api_txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_ids: &[i64],
) -> Result<(), PipelineError> {
    let mut pipeline_ids = pipeline_ids.to_vec();

    // Acquire pipeline locks in a consistent order so competing batches do not
    // each hold a lock the other needs. NOWAIT already prevents waiting on
    // rows.
    pipeline_ids.sort_unstable();
    pipeline_ids.dedup();

    for pipeline_id in pipeline_ids {
        lock_pipeline(api_txn, tenant_id, pipeline_id).await?;
    }

    Ok(())
}
