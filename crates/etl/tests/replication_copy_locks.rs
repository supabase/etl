//! Schema and lock regressions for initial copying.

use std::time::Duration;

use etl::{
    error::{ErrorKind, EtlError},
    postgres::client::{CtidPartition, PgReplicationClient},
    schema::TableId,
    test_utils::{
        database::{
            assert_table_locks_released, spawn_source_database, test_table_name,
            wait_for_table_lock,
        },
        pipeline::test_slot_name,
    },
};
use etl_postgres::{
    below_version, tokio::test_utils::connect_to_pg_database, version::POSTGRES_15,
};
use futures::StreamExt;
use pg_escape::quote_identifier;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, CopyOutStream, error::SqlState, types::Type};

/// Extracts the PostgreSQL error code from a preserved source chain.
fn postgres_error_code(error: &EtlError) -> &SqlState {
    std::iter::successors(std::error::Error::source(error), |source| source.source())
        .find_map(|source| source.downcast_ref::<tokio_postgres::error::DbError>())
        .unwrap()
        .code()
}

/// Waits until slot creation is blocked on the chosen old writer.
async fn wait_for_writer(client: &Client, transaction_id: &str) {
    timeout(Duration::from_secs(10), async {
        loop {
            let row = client
                .query_one(
                    "select exists (select 1 from pg_locks
                     where locktype = 'transactionid' and transactionid::text = $1
                       and mode = 'ShareLock' and not granted)",
                    &[&transaction_id],
                )
                .await
                .unwrap();
            if row.get::<_, bool>(0) {
                return;
            }

            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// Collects a COPY response so assertions cover the actual snapshot rows.
async fn copy_bytes(stream: CopyOutStream) -> Vec<u8> {
    tokio::pin!(stream);
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }

    bytes
}

/// The schema barrier precedes the slot snapshot and preserves normal writes.
#[tokio::test(flavor = "multi_thread")]
async fn copy_locks_precede_snapshot_and_allow_writes() {
    let database = spawn_source_database().await;
    // Exercise identifier quoting in the name-based copy lock.
    let table_id = database
        .create_table(test_table_name(r#"Copy " Source"#), true, &[("age", "integer")])
        .await
        .unwrap();
    database.run_sql(r#"insert into test."Copy "" Source" values (1, 10)"#).await.unwrap();
    database.run_sql("create table test.writer (id int)").await.unwrap();
    database.run_sql("begin").await.unwrap();
    database.run_sql("insert into test.writer values (1)").await.unwrap();

    let (observer, _) = connect_to_pg_database(&database.config).await;
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("locked_copy");

    let release_writer = async {
        wait_for_table_lock(&observer, table_id, "AccessShareLock", true).await;

        // Slot creation cannot complete while the earlier writer is open.
        // Seeing this lock proves it exists before the slot snapshot.
        let error = observer
            .batch_execute(
                r#"begin; lock table test."Copy "" Source" in access exclusive mode nowait"#,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));

        observer.batch_execute("rollback").await.unwrap();
        database.run_sql("commit").await.unwrap();
    };
    let (created, ()) =
        tokio::join!(parent.create_table_copy_slot(&slot_name, table_id, false), release_writer);

    let (transaction, _) = created.unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    let snapshot = transaction.export_snapshot().await.unwrap();

    database.run_sql(r#"update test."Copy "" Source" set age = 20 where id = 1"#).await.unwrap();
    database.run_sql(r#"insert into test."Copy "" Source" values (2, 30)"#).await.unwrap();
    database.run_sql(r#"delete from test."Copy "" Source" where id = 2"#).await.unwrap();
    database.run_sql(r#"vacuum test."Copy "" Source""#).await.unwrap();

    let mut child = transaction.fork_child().await.unwrap();
    let mut child_transaction = child.begin_transaction(&snapshot).await.unwrap();
    let stream = child_transaction
        .get_table_copy_stream_with_ctid_partition(
            table_id,
            table_id,
            &schema.column_schemas,
            None,
            &CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() },
        )
        .await
        .unwrap();
    assert_eq!(copy_bytes(stream).await, b"1\t10\n");

    child_transaction.commit().await.unwrap();
    transaction.commit().await.unwrap();

    assert_table_locks_released(&observer, &schema.name).await;
}

/// A later worker fails immediately behind queued DDL; existing workers can
/// finish, and releasing the copy transactions lets DDL proceed.
#[tokio::test(flavor = "multi_thread")]
async fn copy_worker_rejects_queued_ddl_and_releases_locks() {
    let database = spawn_source_database().await;
    let table_id = database
        .create_table(test_table_name("copied"), true, &[("age", "integer")])
        .await
        .unwrap();
    database.run_sql("insert into test.copied values (1)").await.unwrap();

    let (ddl_client, _) = connect_to_pg_database(&database.config).await;
    let observer = database.client.as_ref().unwrap();
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = parent
        .create_table_copy_slot(&test_slot_name("queued_ddl"), table_id, false)
        .await
        .unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    let snapshot = transaction.export_snapshot().await.unwrap();

    let mut first = transaction.fork_child().await.unwrap();
    let mut first_tx = first.begin_transaction(&snapshot).await.unwrap();
    let partition = CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() };
    let first_stream = first_tx
        .get_table_copy_stream_with_ctid_partition(
            table_id,
            table_id,
            &schema.column_schemas,
            None,
            &partition,
        )
        .await
        .unwrap();
    assert_eq!(copy_bytes(first_stream).await, b"1\t\\N\n");

    let finish_copy = async {
        wait_for_table_lock(observer, table_id, "AccessExclusiveLock", false).await;

        let mut late = transaction.fork_child().await.unwrap();
        let mut late_tx = late.begin_transaction(&snapshot).await.unwrap();
        let error = timeout(
            Duration::from_secs(2),
            late_tx.get_table_copy_stream_with_ctid_partition(
                table_id,
                table_id,
                &schema.column_schemas,
                None,
                &partition,
            ),
        )
        .await
        .unwrap()
        .err()
        .unwrap();
        assert_eq!(error.kind(), ErrorKind::SourceTableCopyLockConflict);

        drop(late_tx);

        let stream = first_tx
            .get_table_copy_stream_with_ctid_partition(
                table_id,
                table_id,
                &schema.column_schemas,
                None,
                &partition,
            )
            .await
            .unwrap();
        assert_eq!(copy_bytes(stream).await, b"1\t\\N\n");

        first_tx.commit().await.unwrap();

        // Exercise rollback on an incomplete copy, rather than committing it.
        drop(transaction);
    };
    let (ddl, ()) = tokio::join!(
        ddl_client.batch_execute(
            "set lock_timeout = '10s'; alter table test.copied alter column id type bigint"
        ),
        finish_copy,
    );
    ddl.unwrap();

    let (transaction, _) =
        parent.create_table_copy_slot(&test_slot_name("after_ddl"), table_id, false).await.unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    assert_eq!(schema.column_schemas[0].typ, Type::INT8);

    transaction.commit().await.unwrap();
}

/// Recursive locks protect empty and nested leaves before workers reach them.
#[tokio::test(flavor = "multi_thread")]
async fn copy_locks_cover_partition_descendants() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.middle partition of test.root for values from (0) to (100)
                 partition by range (id);
             create table test.leaf partition of test.middle for values from (0) to (100)",
        )
        .await
        .unwrap();
    let table_id: TableId =
        client.query_one("select 'test.root'::regclass::oid", &[]).await.unwrap().get(0);

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = parent
        .create_table_copy_slot(&test_slot_name("partition_locks"), table_id, false)
        .await
        .unwrap();

    for name in ["root", "middle", "leaf"] {
        let error = client
            .batch_execute(&format!(
                "begin; lock table only test.{name} in access exclusive mode nowait"
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));

        client.batch_execute("rollback").await.unwrap();
    }

    transaction.commit().await.unwrap();

    assert_table_locks_released(client, &test_table_name("root")).await;
}

/// A partition attached while slot creation waits was not locked before the
/// snapshot and must cause a retry instead of an unsafe copy.
#[tokio::test(flavor = "multi_thread")]
async fn copy_rejects_partition_attached_before_snapshot() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.leaf (id int);
             create table test.writer (id int)",
        )
        .await
        .unwrap();
    client.batch_execute("begin; insert into test.writer values (1)").await.unwrap();
    let table_id: TableId =
        client.query_one("select 'test.root'::regclass::oid", &[]).await.unwrap().get(0);

    let (ddl_client, _) = connect_to_pg_database(&database.config).await;
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let attach = async {
        wait_for_table_lock(&ddl_client, table_id, "AccessShareLock", true).await;

        ddl_client
            .batch_execute(
                "alter table test.root attach partition test.leaf for values from (0) to (100)",
            )
            .await
            .unwrap();
        client.batch_execute("commit").await.unwrap();
    };
    let slot_name = test_slot_name("concurrent_attach");
    let (created, ()) =
        tokio::join!(parent.create_table_copy_slot(&slot_name, table_id, false), attach);

    assert_eq!(created.unwrap_err().kind(), ErrorKind::SourceTableCopyLockConflict);
}

/// A busy root or descendant fails before creating a slot, and releases any
/// locks already acquired for the copy unit.
#[tokio::test(flavor = "multi_thread")]
async fn copy_rejects_busy_table_before_snapshot() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.leaf partition of test.root for values from (0) to (10)",
        )
        .await
        .unwrap();
    let table_id: TableId =
        client.query_one("select 'test.root'::regclass::oid", &[]).await.unwrap().get(0);

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("busy_table");
    for relation in ["root", "leaf"] {
        client
            .batch_execute(&format!(
                "begin; lock table only test.{relation} in access exclusive mode"
            ))
            .await
            .unwrap();
        let error = timeout(
            Duration::from_secs(2),
            parent.create_table_copy_slot(&slot_name, table_id, false),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceTableCopyLockConflict);
        assert_eq!(postgres_error_code(&error), &SqlState::LOCK_NOT_AVAILABLE);

        let slot_exists: bool = client
            .query_one(
                "select exists (select 1 from pg_replication_slots where slot_name = $1)",
                &[&slot_name],
            )
            .await
            .unwrap()
            .get(0);
        assert!(!slot_exists);

        client.batch_execute("rollback").await.unwrap();

        assert_table_locks_released(client, &test_table_name("root")).await;
    }
}

/// Slot creation cannot wait indefinitely for a writer that is itself waiting
/// for the pre-snapshot copy lock.
#[tokio::test(flavor = "multi_thread")]
async fn copy_slot_bounds_writer_ddl_deadlock() {
    let database = spawn_source_database().await;
    let table_id = database
        .create_table(test_table_name("copied"), true, &[("age", "integer")])
        .await
        .unwrap();
    let client = database.client.as_ref().unwrap();
    client.batch_execute("create table test.writer (id int)").await.unwrap();
    client.batch_execute("begin; insert into test.writer values (1)").await.unwrap();
    let transaction_id: String =
        client.query_one("select pg_current_xact_id()::text", &[]).await.unwrap().get(0);

    let (observer, _) = connect_to_pg_database(&database.config).await;
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let writer_ddl = async {
        // Start DDL only once the slot is waiting for this writer. Otherwise
        // the DDL's own XID can enter the slot's initial wait set and turn this
        // application-level cycle into a PostgreSQL-detectable deadlock.
        wait_for_writer(&observer, &transaction_id).await;

        observer.batch_execute("alter table test.copied add column extra int").await.unwrap();
        client.batch_execute("commit").await.unwrap();
    };
    let slot_name = test_slot_name("writer_ddl_cycle");
    let (created, ()) = timeout(Duration::from_secs(45), async {
        tokio::join!(parent.create_table_copy_slot(&slot_name, table_id, false), writer_ddl)
    })
    .await
    .unwrap();

    let error = created.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::SourceTableCopyLockConflict);
    assert_eq!(postgres_error_code(&error), &SqlState::LOCK_NOT_AVAILABLE);

    let (transaction, _) =
        parent.create_table_copy_slot(&slot_name, table_id, false).await.unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    assert_eq!(schema.column_schemas.len(), 3);

    transaction.commit().await.unwrap();
}

/// PostgreSQL-detected slot/DDL deadlocks retry the copy with the source chain
/// intact instead of persisting a non-retryable table failure.
#[tokio::test(flavor = "multi_thread")]
async fn copy_slot_classifies_postgres_deadlock_as_retryable() {
    let database = spawn_source_database().await;
    let table_id = database
        .create_table(test_table_name("copied"), true, &[("age", "integer")])
        .await
        .unwrap();
    let client = database.client.as_ref().unwrap();

    // Let the copy connection detect the cycle first so victim selection is
    // deterministic. This override belongs only to the test's writer.
    client
        .batch_execute(
            "begin; set local deadlock_timeout = '60s'; insert into test.copied values (1)",
        )
        .await
        .unwrap();

    let (observer, _) = connect_to_pg_database(&database.config).await;
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let writer_ddl = async {
        wait_for_table_lock(&observer, table_id, "AccessShareLock", true).await;

        client.batch_execute("alter table test.copied add column extra int; commit").await.unwrap();
    };
    let slot_name = test_slot_name("postgres_deadlock");
    let (created, ()) = timeout(Duration::from_secs(10), async {
        tokio::join!(parent.create_table_copy_slot(&slot_name, table_id, false), writer_ddl)
    })
    .await
    .unwrap();

    let error = created.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::SourceTableCopyLockConflict);
    assert_eq!(postgres_error_code(&error), &SqlState::T_R_DEADLOCK_DETECTED);

    let (transaction, _) =
        parent.create_table_copy_slot(&slot_name, table_id, false).await.unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    assert_eq!(schema.column_schemas.len(), 3);

    transaction.commit().await.unwrap();
}

/// Catalog reads must retain publication column lists and row filters from
/// the slot snapshot even though their DDL does not require ACCESS EXCLUSIVE.
#[tokio::test(flavor = "multi_thread")]
async fn copy_publication_metadata_uses_snapshot() {
    let database = spawn_source_database().await;
    if below_version!(database.server_version(), POSTGRES_15) {
        return;
    }

    let table_id = database
        .create_table(test_table_name("copied"), true, &[("age", "integer"), ("extra", "integer")])
        .await
        .unwrap();
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "insert into test.copied values (1, 10, 100), (2, 20, 200);
             create table test.unrelated (id int);
             create publication copy_pub for table
                 test.copied (id, age) where (age >= 18), test.unrelated",
        )
        .await
        .unwrap();

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = parent
        .create_table_copy_slot(&test_slot_name("publication"), table_id, false)
        .await
        .unwrap();
    let snapshot = transaction.export_snapshot().await.unwrap();

    client
        .batch_execute(
            "alter publication copy_pub set table
                 test.copied (id, extra) where (extra >= 0), test.unrelated;
             begin; lock table test.unrelated in access exclusive mode",
        )
        .await
        .unwrap();

    let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
    // Publication expansion would open unrelated tables and block on this
    // session. The copy's catalog query must only inspect its own copy unit.
    let columns = timeout(
        Duration::from_secs(2),
        transaction.get_replicated_column_names(table_id, &schema, "copy_pub"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(columns, ["id".to_owned(), "age".to_owned()].into_iter().collect());

    let column_schemas = schema
        .column_schemas
        .into_iter()
        .filter(|column| columns.contains(&column.name))
        .collect::<Vec<_>>();
    let mut child = transaction.fork_child().await.unwrap();
    let mut child_tx = child.begin_transaction(&snapshot).await.unwrap();
    let stream = child_tx
        .get_table_copy_stream_with_ctid_partition(
            table_id,
            table_id,
            &column_schemas,
            Some("copy_pub"),
            &CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() },
        )
        .await
        .unwrap();
    assert_eq!(copy_bytes(stream).await, b"2\t20\n");

    child_tx.commit().await.unwrap();
    transaction.commit().await.unwrap();
    client.batch_execute("rollback").await.unwrap();
}

/// Snapshot catalog queries agree with PostgreSQL's publication expansion for
/// roots, subtrees, leaves, schema membership, and ordinary inheritance.
#[tokio::test(flavor = "multi_thread")]
async fn copy_publication_columns_match_postgres() {
    let database = spawn_source_database().await;
    if below_version!(database.server_version(), POSTGRES_15) {
        return;
    }

    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            r#"create schema copy_roots;
               create schema copy_branches;
               create schema copy_leaves;
               create table copy_roots.root (id int, discarded int, "odd "" column" int)
                   partition by range (id);
               create table copy_branches.branch partition of copy_roots.root
                   for values from (0) to (100) partition by range (id);
               create table copy_leaves.leaf partition of copy_branches.branch
                   for values from (0) to (100);
               alter table copy_roots.root drop column discarded;
               create table test.parent (id int, "odd "" column" int);
               create table test.child () inherits (test.parent)"#,
        )
        .await
        .unwrap();

    let mut publications = Vec::new();
    for via_root in [false, true] {
        // PostgreSQL forbids column lists on partitioned tables unless they
        // supply the published root identity.
        let root_columns = if via_root { " (id)" } else { "" };
        let branch_columns = if via_root { r#" ("odd "" column")"# } else { "" };
        for (case, targets) in [
            ("root", format!("table copy_roots.root{root_columns}, test.parent (id)")),
            ("subtree", format!("table copy_branches.branch{branch_columns}")),
            ("leaf", r#"table copy_leaves.leaf ("odd "" column")"#.to_owned()),
            (
                "overlap",
                format!(
                    r#"table copy_roots.root{root_columns}, copy_leaves.leaf ("odd "" column")"#
                ),
            ),
            ("all", "all tables".to_owned()),
            ("root_schema", "tables in schema copy_roots".to_owned()),
            ("leaf_schema", "tables in schema copy_leaves".to_owned()),
            (
                "mixed_schema",
                "table copy_leaves.leaf, test.parent, tables in schema copy_roots".to_owned(),
            ),
        ] {
            let publication = format!("copy_{case}_{via_root}");
            client
                .batch_execute(&format!(
                    "create publication {} for {targets}
                     with (publish_via_partition_root = {via_root})",
                    quote_identifier(&publication),
                ))
                .await
                .unwrap();
            publications.push(publication);
        }
    }

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("publication_rules");
    for relation in
        ["copy_roots.root", "copy_branches.branch", "copy_leaves.leaf", "test.parent", "test.child"]
    {
        let table_id: TableId =
            client.query_one("select $1::text::regclass::oid", &[&relation]).await.unwrap().get(0);
        let (transaction, _) =
            parent.create_table_copy_slot(&slot_name, table_id, false).await.unwrap();
        let (schema, _) = transaction.get_table_schema_with_identity(table_id).await.unwrap();
        for publication in &publications {
            // Use the server's view as an independent oracle while this
            // fixture's catalogs are stable.
            let expected = client
                .query_opt(
                    "select pt.attnames::text[] from pg_publication_tables pt
                     join pg_namespace n on n.nspname = pt.schemaname
                     join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename
                     where pt.pubname = $1 and c.oid = $2",
                    &[publication, &table_id],
                )
                .await
                .unwrap();
            let actual =
                transaction.get_replicated_column_names(table_id, &schema, publication).await;
            if let Some(expected) = expected {
                let columns = expected.get::<_, Vec<String>>(0).into_iter().collect();
                assert_eq!(actual.unwrap(), columns, "{publication}: {relation}");
            } else {
                assert_eq!(
                    actual.unwrap_err().kind(),
                    ErrorKind::ConfigError,
                    "{publication}: {relation}"
                );
            }
        }

        transaction.commit().await.unwrap();
        parent.delete_slot_if_exists(&slot_name).await.unwrap();
    }
}

/// Later attachments cannot add unprotected physical sources to the plan.
#[tokio::test(flavor = "multi_thread")]
async fn copy_partition_plan_uses_snapshot() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.first partition of test.root for values from (0) to (10);
             create table test.later (id int)",
        )
        .await
        .unwrap();
    let row = client
        .query_one("select 'test.root'::regclass::oid, 'test.first'::regclass::oid", &[])
        .await
        .unwrap();
    let table_id: TableId = row.get(0);
    let first_id: TableId = row.get(1);

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = parent
        .create_table_copy_slot(&test_slot_name("partition_plan"), table_id, false)
        .await
        .unwrap();

    client
        .batch_execute(
            "alter table test.root attach partition test.later for values from (10) to (20)",
        )
        .await
        .unwrap();
    assert_eq!(transaction.get_leaf_partitions(table_id).await.unwrap(), vec![first_id]);

    transaction.commit().await.unwrap();
}

/// A foreign leaf cannot silently disappear from a partitioned root's copy.
#[tokio::test(flavor = "multi_thread")]
async fn copy_partition_plan_rejects_foreign_leaf() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.local_leaf partition of test.root for values from (0) to (10);
             create foreign data wrapper copy_test_fdw;
             create server copy_test_server foreign data wrapper copy_test_fdw;
             create foreign table test.remote_leaf partition of test.root
                 for values from (10) to (20) server copy_test_server;
             create publication copy_pub for table test.root
                 with (publish_via_partition_root = true)",
        )
        .await
        .unwrap();
    let table_id: TableId =
        client.query_one("select 'test.root'::regclass::oid", &[]).await.unwrap().get(0);

    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = parent
        .create_table_copy_slot(&test_slot_name("foreign_leaf"), table_id, false)
        .await
        .unwrap();

    let error = transaction.get_leaf_partitions(table_id).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::SourceSchemaError);

    transaction.commit().await.unwrap();
}

/// A root row-filter deparser must not wait behind queued root DDL even when
/// the physical leaf itself is available to a new copy worker.
#[tokio::test(flavor = "multi_thread")]
async fn copy_worker_rejects_queued_root_ddl() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id int) partition by range (id);
             create table test.leaf partition of test.root for values from (0) to (10);
             create publication copy_pub for table test.root with (publish_via_partition_root = \
             true)",
        )
        .await
        .unwrap();
    if !below_version!(database.server_version(), POSTGRES_15) {
        client
            .batch_execute("alter publication copy_pub set table test.root where (id >= 0)")
            .await
            .unwrap();
    }
    let row = client
        .query_one("select 'test.root'::regclass::oid, 'test.leaf'::regclass::oid", &[])
        .await
        .unwrap();
    let root_id: TableId = row.get(0);
    let leaf_id: TableId = row.get(1);

    let (ddl_client, _) = connect_to_pg_database(&database.config).await;
    let mut parent = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) =
        parent.create_table_copy_slot(&test_slot_name("root_ddl"), root_id, false).await.unwrap();
    let snapshot = transaction.export_snapshot().await.unwrap();
    let (schema, _) = transaction.get_table_schema_with_identity(root_id).await.unwrap();

    let fail_copy = async {
        wait_for_table_lock(client, root_id, "AccessExclusiveLock", false).await;

        let mut child = transaction.fork_child().await.unwrap();
        let mut child_tx = child.begin_transaction(&snapshot).await.unwrap();
        let error = timeout(
            Duration::from_secs(2),
            child_tx.get_table_copy_stream_with_ctid_partition(
                leaf_id,
                root_id,
                &schema.column_schemas,
                Some("copy_pub"),
                &CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() },
            ),
        )
        .await
        .unwrap()
        .err()
        .unwrap();
        assert_eq!(error.kind(), ErrorKind::SourceTableCopyLockConflict);

        drop(child_tx);
        drop(transaction);
    };
    let (ddl, ()) = tokio::join!(
        ddl_client
            .batch_execute("set lock_timeout = '10s'; alter table test.root add column extra int"),
        fail_copy
    );
    ddl.unwrap();
}
