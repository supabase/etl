//! Integration tests against a Multigres cluster.

use etl::{
    config::{ReplicationSlotConfig, ReplicationSlotPersistence},
    postgres::client::PgReplicationClient,
    test_utils::pipeline::test_slot_name,
};

mod pipeline_failover;
mod support;

/// Release accounting must find the creator even before its slot is visible.
#[tokio::test]
#[ignore = "requires the isolated Multigres cluster"]
async fn table_sync_backend_pid_identifies_the_live_postgres_session() {
    let gateway = support::Multigres::new().await;
    let mut owner = PgReplicationClient::connect(gateway.config.clone()).await.unwrap();
    let pid = owner.backend_pid().await.unwrap();
    let observer = gateway.client.as_ref().unwrap();
    let live: bool = observer
        .query_one(
            "select exists (select 1 from pg_stat_activity where pid = $1 and backend_type = \
             'walsender')",
            &[&pid],
        )
        .await
        .unwrap()
        .get(0);
    assert!(live);

    let name = test_slot_name("multigres_sync_pid");
    owner
        .create_slot(
            &name,
            &ReplicationSlotConfig {
                persistence: ReplicationSlotPersistence::Temporary,
                failover: false,
            },
        )
        .await
        .unwrap();
    let active_pid: i32 = observer
        .query_one("select active_pid from pg_replication_slots where slot_name = $1", &[&name])
        .await
        .unwrap()
        .get(0);
    assert_eq!(pid, active_pid);
}
