use tokio_postgres::Transaction;

use crate::tokio::{PgSourceError, health, store};

/// Deletes all source-side metadata for a pipeline.
pub async fn delete_pipeline_source_state(
    txn: &Transaction<'_>,
    pipeline_id: i64,
) -> Result<(), PgSourceError> {
    if health::etl_tables_present(txn).await? {
        store::delete_replication_state_for_all_tables(txn, pipeline_id).await?;
        store::delete_table_schemas_for_all_tables(txn, pipeline_id).await?;
        store::delete_destination_tables_metadata_for_all_tables(txn, pipeline_id).await?;
    }

    Ok(())
}

/// Deletes all source-side metadata for multiple pipelines.
pub async fn delete_pipelines_source_state(
    txn: &Transaction<'_>,
    pipeline_ids: &[i64],
) -> Result<Vec<i64>, PgSourceError> {
    let mut deleted_pipeline_ids = Vec::with_capacity(pipeline_ids.len());
    for pipeline_id in pipeline_ids {
        delete_pipeline_source_state(txn, *pipeline_id).await?;
        deleted_pipeline_ids.push(*pipeline_id);
    }

    Ok(deleted_pipeline_ids)
}

/// Uninstalls current-user-owned ETL source objects.
pub async fn uninstall_source_installation(txn: &Transaction<'_>) -> Result<(), PgSourceError> {
    let trigger_owned_by_current_user: bool = txn
        .query_one(
            r#"
            select exists(
                select 1
                from pg_event_trigger
                where evtname = 'supabase_etl_ddl_message_trigger'
                  and evtowner = (select oid from pg_roles where rolname = current_user)
            )
            "#,
            &[],
        )
        .await?
        .get(0);

    if trigger_owned_by_current_user {
        txn.execute("drop event trigger if exists supabase_etl_ddl_message_trigger", &[]).await?;
    }

    let etl_schema_owned_by_current_user: bool = txn
        .query_one(
            r#"
            select exists(
                select 1
                from pg_namespace
                where nspname = 'etl'
                  and nspowner = (select oid from pg_roles where rolname = current_user)
            )
            "#,
            &[],
        )
        .await?
        .get(0);

    if etl_schema_owned_by_current_user {
        txn.execute("drop schema if exists etl cascade", &[]).await?;
    }

    Ok(())
}
