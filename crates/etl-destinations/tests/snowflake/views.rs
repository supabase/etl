use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    destination::{Destination as _, DestinationTableMetadata, WriteEventsDurability},
    event::{Event, RelationEvent},
    schema::{
        ColumnSchema, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableSchema, Type,
    },
    store::{SchemaStore, StateStore, TableState, TableStateLifecycleStore},
    test_utils::{
        destination::{drop_table_for_copy, write_events, write_table_rows},
        notifying_store::NotifyingStore,
    },
};
use etl_destinations::snowflake::{Client, Config, Destination, SqlClient, test_utils::query_rows};
use serde_json::json;

use crate::snowflake::common::{
    build_auth, companion_name, companion_query, quote, with_table_cleanup,
};

/// Covers view ownership, restart recovery, grants, mappings, and raw-only
/// tables with three tiny tables. Row correctness across real pgoutput DDL is
/// also checked by the DEFAULT/FULL integration workloads.
#[tokio::test]
#[ignore = "requires Snowflake test credentials"]
async fn companion_view_lifecycle_and_ownership() {
    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let name = format!("views{}", uuid::Uuid::new_v4().simple());
    let physical = format!("PUBLIC_{name}").to_uppercase();
    let keyless_name = format!("keyless{}", uuid::Uuid::new_v4().simple());
    let keyless_physical = format!("PUBLIC_{keyless_name}").to_uppercase();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let long_schema = format!("{}{}{}", &nonce[..1], "_".repeat(61), &nonce[1..2]);
    let long_table = format!("{}{}{}", &nonce[2..3], "_".repeat(61), &nonce[3..4]);
    let long_physical =
        format!("{}_{}", long_schema.replace('_', "__"), long_table.replace('_', "__"))
            .to_uppercase();
    let fqn = |name: &str| {
        format!("{}.{}.{}", quote(config.database()), quote(config.schema()), quote(name))
    };
    let public_name = companion_name(&physical);
    let public = fqn(&public_name);
    let pending = fqn(&format!("{physical}_CURRENT_PENDING"));
    let query = companion_query(&config, &physical);
    let original = TableSchema::new(
        TableId::new(1178),
        TableName::new("public".into(), name),
        vec![
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 3, true),
        ],
    );
    let schema = ReplicatedTableSchema::all(Arc::new(original.clone()));
    let store = NotifyingStore::new();
    store.store_table_schema(original).await.unwrap();
    let fresh = || Destination::new(Client::new(Arc::clone(&auth), 1178), store.clone());
    let destination = fresh();
    let cleanup_tables: [&str; 4] = [&physical, &keyless_physical, &long_physical, &public_name];
    with_table_cleanup(&sql, &cleanup_tables, || async {
        // A foreign table occupying the view name must survive a failed setup.
        sql.execute_ddl(&format!("create transient table {public} (sentinel integer)"))
            .await
            .unwrap();
        assert!(write_table_rows(&destination, &schema, vec![]).await.is_err());
        assert!(
            store.get_destination_table_metadata(schema.id()).await.unwrap().unwrap().is_creating()
        );
        assert!(
            query_rows(&sql, &format!("select sentinel from {public}")).await.unwrap().is_empty()
        );
        sql.execute_ddl(&format!("drop table {public}")).await.unwrap();
        // Recovery must not skip the view just because the channel is warm.
        write_table_rows(
            &destination,
            &schema,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("kept".into())])],
        )
        .await
        .unwrap();
        write_table_rows(&destination, &schema, vec![]).await.unwrap();
        store.update_table_state(schema.id(), TableState::Ready).await.unwrap();
        let expected = query_rows(&sql, &query).await.unwrap();
        assert_eq!(&expected[0][..2], &[json!("1"), json!("kept")]);

        // A caller's current schema must not redirect the view to another
        // table.
        let mut other_config =
            Config::new(config.account(), config.user(), config.database(), "INFORMATION_SCHEMA")
                .unwrap();
        if let Some(role) = config.role() {
            other_config = other_config.with_role(role);
        }
        let other_sql = SqlClient::new(other_config, Arc::clone(&auth), reqwest::Client::new());
        assert_eq!(query_rows(&other_sql, &query).await.unwrap(), expected);

        // Grant only to the already owning test role; no wider access is added.
        let role = query_rows(&sql, "select current_role()").await.unwrap()[0][0]
            .as_str()
            .unwrap()
            .to_owned();
        sql.execute_ddl(&format!("grant select on view {public} to role {}", quote(&role)))
            .await
            .unwrap();
        let mapping = query_rows(&sql, &format!("describe view {public}")).await.unwrap();
        assert!(mapping.iter().any(|row| row.iter().filter_map(|value| value.as_str()).any(
            |value| {
                serde_json::from_str::<serde_json::Value>(value)
                    .is_ok_and(|metadata| metadata["column_id"] == 3)
            }
        )));

        let applied = store.get_destination_table_metadata(schema.id()).await.unwrap().unwrap();
        // Interrupted after parking but before raw DDL: startup restores the
        // applied schema even if this table receives no subsequent events.
        sql.execute_ddl(&format!("alter view {public} rename to {pending}")).await.unwrap();
        fresh().startup().await.unwrap();
        assert_eq!(query_rows(&sql, &query).await.unwrap(), expected);

        let mut renamed_columns = schema.inner().column_schemas.clone();
        renamed_columns[1].name = "details\"'".into();
        let renamed = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            renamed_columns,
            SnapshotId::new(10_u64.into(), 11_u64.into()),
        )));
        store.store_table_schema(renamed.inner().clone()).await.unwrap();
        let applying = applied
            .clone()
            .with_schema_change(renamed.inner().snapshot_id, renamed.replication_mask().clone())
            .unwrap();
        sql.execute_ddl(&format!("alter view {public} rename to {pending}")).await.unwrap();
        store.store_destination_table_metadata(schema.id(), applying.clone()).await.unwrap();
        fresh().startup().await.unwrap();
        assert!(query_rows(&sql, &query).await.is_err());
        assert!(write_table_rows(&fresh(), &renamed, vec![]).await.is_err());
        // Interrupted after raw Applied but before view replacement.
        sql.execute_ddl(&format!(
            "alter table {} rename column \"payload\" to {}",
            fqn(&physical),
            quote("details\"'")
        ))
        .await
        .unwrap();
        store.store_destination_table_metadata(schema.id(), applying.to_applied()).await.unwrap();
        fresh().startup().await.unwrap();
        assert_eq!(query_rows(&sql, &query).await.unwrap(), expected);
        assert_eq!(
            query_rows(&sql, &format!("select {} from {public}", quote("details\"'")))
                .await
                .unwrap(),
            vec![vec![json!("kept")]]
        );

        // The replaced definition may also be left under the pending name.
        sql.execute_ddl(&format!("alter view {public} rename to {pending}")).await.unwrap();
        fresh().startup().await.unwrap();
        assert_eq!(query_rows(&sql, &query).await.unwrap(), expected);

        // Add-only DDL updates the public definition without leaving a pending
        // object. Existing source values and explicit grants survive.
        let mut added_columns = renamed.inner().column_schemas.clone();
        added_columns.push(ColumnSchema::new("added".into(), Type::TEXT, -1, 4, true));
        let added = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            added_columns,
            SnapshotId::new(20_u64.into(), 21_u64.into()),
        )));
        store.store_table_schema(added.inner().clone()).await.unwrap();
        write_events(
            &fresh(),
            WriteEventsDurability::RequireDurable,
            vec![Event::Relation(RelationEvent { replicated_table_schema: added.clone() })],
        )
        .await
        .unwrap();
        assert_eq!(
            query_rows(&sql, &format!("select \"added\" from {public}")).await.unwrap(),
            vec![vec![json!(null)]]
        );
        assert!(query_rows(&sql, &format!("select * from {pending}")).await.is_err());
        let grants = query_rows(&sql, &format!("show grants on view {public}")).await.unwrap();
        assert!(grants.iter().any(|row| row[1] == "SELECT" && row[5] == role));

        // A parking collision must fail before Applying or raw DDL, preserving
        // both the old view and the foreign object.
        let mut blocked_columns = added.inner().column_schemas.clone();
        blocked_columns[1].name = "blocked".into();
        let blocked = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            blocked_columns,
            SnapshotId::new(30_u64.into(), 31_u64.into()),
        )));
        store.store_table_schema(blocked.inner().clone()).await.unwrap();
        sql.execute_ddl(&format!("create view {pending} as select 42 as sentinel")).await.unwrap();
        assert!(
            write_events(
                &fresh(),
                WriteEventsDurability::RequireDurable,
                vec![Event::Relation(RelationEvent { replicated_table_schema: blocked })],
            )
            .await
            .is_err()
        );
        let metadata = store.get_destination_table_metadata(schema.id()).await.unwrap().unwrap();
        assert!(metadata.is_applied());
        assert_eq!(metadata.snapshot_id(), added.inner().snapshot_id);
        assert_eq!(
            query_rows(&sql, &format!("select {} from {public}", quote("details\"'")))
                .await
                .unwrap(),
            vec![vec![json!("kept")]]
        );
        assert_eq!(
            query_rows(&sql, &format!("select sentinel from {pending}")).await.unwrap(),
            vec![vec![json!("42")]]
        );
        sql.execute_ddl(&format!("drop view {pending}")).await.unwrap();

        // Conflicting public/pending objects are never silently overwritten.
        sql.execute_ddl(&format!("alter view {public} rename to {pending}")).await.unwrap();
        sql.execute_ddl(&format!("create view {public} as select 42 as sentinel")).await.unwrap();
        assert!(fresh().startup().await.is_err());
        assert_eq!(
            query_rows(&sql, &format!("select sentinel from {public}")).await.unwrap(),
            vec![vec![json!("42")]]
        );
        sql.execute_ddl(&format!("drop view {public}")).await.unwrap();
        fresh().startup().await.unwrap();

        // This name is legal for raw CDC, but reserved by the query helper.
        // Failed restoration must retain Applied raw metadata and the parked
        // view, and must not be bypassed by an already open channel.
        let mut unsupported_columns = added.inner().column_schemas.clone();
        unsupported_columns[1].name = "_cdc_custom".into();
        let unsupported = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            unsupported_columns,
            SnapshotId::new(40_u64.into(), 41_u64.into()),
        )));
        store.store_table_schema(unsupported.inner().clone()).await.unwrap();
        let failed = fresh();
        assert!(
            write_events(
                &failed,
                WriteEventsDurability::RequireDurable,
                vec![Event::Relation(RelationEvent {
                    replicated_table_schema: unsupported.clone()
                })],
            )
            .await
            .is_err()
        );
        let metadata = store.get_destination_table_metadata(schema.id()).await.unwrap().unwrap();
        assert!(metadata.is_applied());
        assert_eq!(metadata.snapshot_id(), unsupported.inner().snapshot_id);
        assert_eq!(
            query_rows(&sql, &format!("select \"_cdc_custom\" from {}", fqn(&physical)))
                .await
                .unwrap(),
            vec![vec![json!("kept")]]
        );
        assert!(query_rows(&sql, &query).await.is_err());
        assert_eq!(
            query_rows(
                &sql,
                &format!(
                    "show views in schema {}.{} starts with '{physical}_CURRENT_PENDING'",
                    quote(config.database()),
                    quote(config.schema())
                )
            )
            .await
            .unwrap()
            .len(),
            1
        );
        let error = write_table_rows(&failed, &unsupported, vec![]).await.unwrap_err();
        assert_eq!(error.kind(), etl::error::ErrorKind::ConfigError);

        // Same-name resync keeps the grants parked until the new raw generation
        // exists; old row values cannot leak into that generation.
        let reset = fresh();
        store.reset_table_states_for_resync().await.unwrap();
        // Reset retains old metadata for cleanup. Startup must not restore its
        // incompatible view or require the raw table after the drop succeeds.
        reset.startup().await.unwrap();
        drop_table_for_copy(&reset, &unsupported).await.unwrap();
        fresh().startup().await.unwrap();
        assert!(query_rows(&sql, &query).await.is_err());
        store.prepare_table_state_for_copy(schema.id()).await.unwrap();
        store.store_table_schema(schema.inner().clone()).await.unwrap();
        write_table_rows(
            &reset,
            &schema,
            vec![TableRow::new(vec![Cell::I32(2), Cell::String("fresh".into())])],
        )
        .await
        .unwrap();
        write_table_rows(&reset, &schema, vec![]).await.unwrap();
        let records = query_rows(&sql, &query).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(&records[0][..2], &[json!("2"), json!("fresh")]);
        let grants = query_rows(&sql, &format!("show grants on view {public}")).await.unwrap();
        assert!(grants.iter().any(|row| row[1] == "SELECT" && row[5] == role));

        // A source rename can be valid in PostgreSQL but unsupported when
        // naming a new destination. Reopen and reset still use the owned name.
        let source_renamed = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            schema.id(),
            TableName::new("public".into(), "renamed\"source".into()),
            schema.inner().column_schemas.clone(),
            SnapshotId::new(45_u64.into(), 46_u64.into()),
        )));
        store.store_table_schema(source_renamed.inner().clone()).await.unwrap();
        write_events(
            &reset,
            WriteEventsDurability::RequireDurable,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: source_renamed.clone(),
            })],
        )
        .await
        .unwrap();
        let reopened = fresh();
        write_events(
            &reopened,
            WriteEventsDurability::RequireDurable,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: source_renamed.clone(),
            })],
        )
        .await
        .unwrap();
        assert_eq!(query_rows(&sql, &query).await.unwrap(), records);
        drop_table_for_copy(&fresh(), &source_renamed).await.unwrap();
        assert!(!sql.table_exists(&physical).await.unwrap());
        assert!(query_rows(&sql, &query).await.is_err());
        store.prepare_table_state_for_copy(schema.id()).await.unwrap();
        let error = write_table_rows(&fresh(), &source_renamed, vec![]).await.unwrap_err();
        assert_eq!(error.kind(), etl::error::ErrorKind::ValidationError);
        assert!(store.get_destination_table_metadata(schema.id()).await.unwrap().is_none());

        // Maximum-length PostgreSQL components can encode to a 249-character
        // raw name. The reserved view suffix must not reject such a table.
        assert_eq!(long_physical.len(), 249);
        let long = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1180),
            TableName::new(long_schema, long_table),
            vec![ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1)],
        )));
        store.store_table_schema(long.inner().clone()).await.unwrap();
        // Seed an applied raw table directly to isolate SQL/view identifier
        // handling from the separate Snowpipe pipe/channel naming limits.
        sql.create_table_if_not_exists(
            &long_physical,
            "\"id\" integer, \"_cdc_operation\" varchar not null, \"_cdc_sequence_number\" \
             varchar not null, \"_cdc_missing_columns\" array",
        )
        .await
        .unwrap();
        store
            .store_destination_table_metadata(
                long.id(),
                DestinationTableMetadata::new_applied(
                    long_physical.clone(),
                    long.inner().snapshot_id,
                    long.replication_mask().clone(),
                ),
            )
            .await
            .unwrap();
        store
            .update_table_state(
                long.id(),
                TableState::SyncDone { lsn: 0_u64.into(), table_decoding_state: None },
            )
            .await
            .unwrap();
        fresh().startup().await.unwrap();
        assert!(
            query_rows(&sql, &companion_query(&config, &long_physical)).await.unwrap().is_empty()
        );
        let long_public = fqn(&companion_name(&long_physical));
        let long_pending = fqn(&format!("{}_PENDING", companion_name(&long_physical)));
        sql.execute_ddl(&format!("alter view {long_public} rename to {long_pending}"))
            .await
            .unwrap();
        fresh().startup().await.unwrap();
        assert!(
            query_rows(&sql, &companion_query(&config, &long_physical)).await.unwrap().is_empty()
        );

        let keyless = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1179),
            TableName::new("public".into(), keyless_name),
            vec![ColumnSchema::new("value".into(), Type::TEXT, -1, 1, true)],
        )));
        store.store_table_schema(keyless.inner().clone()).await.unwrap();
        write_table_rows(&reset, &keyless, vec![]).await.unwrap();
        store.update_table_state(keyless.id(), TableState::Ready).await.unwrap();
        assert!(sql.table_exists(&keyless_physical).await.unwrap());
        assert!(query_rows(&sql, &companion_query(&config, &keyless_physical)).await.is_err());
        // Raw-only tables do not reserve view names. Customer objects at either
        // name must survive startup, source DDL, and a table resync.
        let keyless_view = fqn(&companion_name(&keyless_physical));
        let keyless_pending = fqn(&format!("{}_PENDING", companion_name(&keyless_physical)));
        for name in [&keyless_view, &keyless_pending] {
            sql.execute_ddl(&format!("create view {name} as select 42 as sentinel")).await.unwrap();
        }
        fresh().startup().await.unwrap();
        let keyless_renamed = ReplicatedTableSchema::all(Arc::new(TableSchema::with_snapshot_id(
            keyless.id(),
            keyless.name().clone(),
            vec![ColumnSchema::new("renamed".into(), Type::TEXT, -1, 1, true)],
            SnapshotId::new(50_u64.into(), 51_u64.into()),
        )));
        store.store_table_schema(keyless_renamed.inner().clone()).await.unwrap();
        write_events(
            &reset,
            WriteEventsDurability::RequireDurable,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: keyless_renamed.clone(),
            })],
        )
        .await
        .unwrap();
        assert!(
            query_rows(&sql, &format!("select \"renamed\" from {}", fqn(&keyless_physical)))
                .await
                .unwrap()
                .is_empty()
        );
        store.reset_table_states_for_resync().await.unwrap();
        drop_table_for_copy(&reset, &keyless_renamed).await.unwrap();
        store.prepare_table_state_for_copy(keyless.id()).await.unwrap();
        store.store_table_schema(keyless.inner().clone()).await.unwrap();
        write_table_rows(&reset, &keyless, vec![]).await.unwrap();
        for name in [&keyless_view, &keyless_pending] {
            assert_eq!(
                query_rows(&sql, &format!("select sentinel from {name}")).await.unwrap(),
                vec![vec![json!("42")]]
            );
        }
    })
    .await;
}
