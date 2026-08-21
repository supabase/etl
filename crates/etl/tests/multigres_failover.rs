//! Manual end-to-end test: run a real ETL pipeline against a Multigres cluster
//! (through the multigateway) using **failover** replication slots, and verify
//! it survives a primary failover with no committed-data loss.
//!
//! `#[ignore]`d — needs a running Multigres cluster with slot-based replication
//! enabled, reachable through the multigateway. It does NOT use `PgDatabase`
//! (which creates a per-test database, unsupported through the gateway); it
//! drives a fixed database directly.
//!
//! A background writer inserts continuously (so the failover slot's
//! `catalog_xmin` keeps advancing and it stays syncable), and the failover is
//! triggered only once the slot is `failover_ready` on the standbys — the
//! `MULTIGRES_FAILOVER_CMD` is responsible for that wait before killing the
//! primary.
//!
//! Env: MULTIGRES_GW_HOST (127.0.0.1), MULTIGRES_GW_PORT (15432),
//!      MULTIGRES_GW_USER (postgres), MULTIGRES_GW_PASSWORD (postgres),
//!      MULTIGRES_GW_DBNAME (postgres), MULTIGRES_FAILOVER_CMD (optional).
//!
//! Run:
//!   cargo test -p etl --test multigres_failover --features test-utils \
//!     -- --ignored --nocapture

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use etl::config::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use etl::event::Event;
use etl::store::MemoryStore;
use etl::test_utils::memory_destination::MemoryDestination;
use etl::test_utils::pipeline::PipelineBuilder;
use tokio_postgres::{Client, NoTls};

const TABLE: &str = "etl_failover";
const PUBLICATION: &str = "etl_failover_pub";
const PIPELINE_ID: u64 = 987_654;
const SEED_ROWS: i64 = 10;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn gw_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: env_or("MULTIGRES_GW_HOST", "127.0.0.1"),
        hostaddr: None,
        port: env_or("MULTIGRES_GW_PORT", "15432").parse().expect("port"),
        name: env_or("MULTIGRES_GW_DBNAME", "postgres"),
        username: env_or("MULTIGRES_GW_USER", "postgres"),
        password: Some(env_or("MULTIGRES_GW_PASSWORD", "postgres").into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
        keepalive: TcpKeepaliveConfig::default(),
    }
}

/// Opens a plain (non-replication) client to the gateway; `None` on failure.
async fn try_connect() -> Option<Client> {
    let host = env_or("MULTIGRES_GW_HOST", "127.0.0.1");
    let user = env_or("MULTIGRES_GW_USER", "postgres");
    let password = env_or("MULTIGRES_GW_PASSWORD", "postgres");
    let dbname = env_or("MULTIGRES_GW_DBNAME", "postgres");
    let port: u16 = env_or("MULTIGRES_GW_PORT", "15432").parse().expect("port");
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user(&user).password(&password).dbname(&dbname);
    match cfg.connect(NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Some(client)
        }
        Err(_) => None,
    }
}

async fn connect() -> Client {
    for _ in 0..30 {
        if let Some(c) = try_connect().await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!("could not connect to the gateway");
}

async fn insert_events(dest: &MemoryDestination<MemoryStore>) -> usize {
    dest.events().await.iter().filter(|e| matches!(e, Event::Insert(_))).count()
}

async fn copied_rows(dest: &MemoryDestination<MemoryStore>) -> usize {
    dest.table_rows().await.values().map(|v| v.len()).sum()
}

async fn source_count(client: &Client) -> i64 {
    client.query_one(&format!("SELECT count(*) FROM {TABLE}"), &[]).await.unwrap().get(0)
}

async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while !f().await {
        assert!(start.elapsed() <= timeout, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Multigres cluster reachable via the multigateway"]
async fn pipeline_survives_primary_failover_with_failover_slots() {
    // 1. Setup the source through the gateway: fresh table + publication + seed.
    let client = connect().await;
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {TABLE} CASCADE;
             CREATE TABLE {TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
             DROP PUBLICATION IF EXISTS {PUBLICATION};
             CREATE PUBLICATION {PUBLICATION} FOR TABLE {TABLE};
             INSERT INTO {TABLE} SELECT g, 'seed-'||g FROM generate_series(1, {SEED_ROWS}) g;"
        ))
        .await
        .expect("source setup");

    // 2. Build and start a real pipeline with failover slots. `start` is where
    //    the FAILOVER slot is created THROUGH the gateway — without the FAILOVER
    //    option the gateway would reject it.
    let store = MemoryStore::new();
    let destination = MemoryDestination::new(store.clone());
    let mut pipeline = PipelineBuilder::new(
        gw_config(),
        PIPELINE_ID,
        PUBLICATION.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_failover(true)
    // Survive the connection drops a failover causes (a production replicator
    // is restarted by its supervisor and resumes from the slot; here the
    // single-process pipeline must retry in place).
    .with_retry_config(2000, 60)
    .build();
    pipeline.start().await.expect("pipeline start (failover slot admitted by gateway)");

    // 3. Initial copy must deliver the seed rows.
    wait_until("initial copy", Duration::from_secs(60), || async {
        copied_rows(&destination).await >= SEED_ROWS as usize
    })
    .await;
    println!("initial copy complete: {} rows", copied_rows(&destination).await);

    // 4. Continuous writer: keeps inserting (so the slot stays syncable) and
    //    reconnects across the failover window.
    let stop = Arc::new(AtomicBool::new(false));
    let next_id = Arc::new(AtomicI64::new(SEED_ROWS + 1));
    let writer = tokio::spawn({
        let stop = stop.clone();
        let next_id = next_id.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let Some(client) = try_connect().await else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let note = format!("w-{id}");
                    if client
                        .execute(
                            &format!("INSERT INTO {TABLE}(id, note) VALUES ($1, $2)"),
                            &[&id, &note],
                        )
                        .await
                        .is_err()
                    {
                        break; // connection broke (failover) — reconnect
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            }
        }
    });

    // 5. Confirm streaming works.
    wait_until("streaming before failover", Duration::from_secs(60), || async {
        insert_events(&destination).await >= 30
    })
    .await;
    let before = insert_events(&destination).await;
    println!("streaming before failover: {before} insert events");

    // 6. Trigger the failover (the command waits for the slot to be
    //    failover_ready on the standbys, then kills the primary).
    if let Ok(cmd) = std::env::var("MULTIGRES_FAILOVER_CMD") {
        println!("triggering failover: {cmd}");
        let status = tokio::task::spawn_blocking(move || {
            std::process::Command::new("sh").arg("-c").arg(&cmd).status()
        })
        .await
        .unwrap()
        .expect("run MULTIGRES_FAILOVER_CMD");
        assert!(status.success(), "failover command failed");
    } else {
        println!("MULTIGRES_FAILOVER_CMD not set — verifying streaming only");
    }

    // 7. The consumer must RESUME after the failover: more inserts arrive with
    //    no re-seed.
    wait_until("streaming resumes after failover", Duration::from_secs(180), || async {
        insert_events(&destination).await >= before + 30
    })
    .await;
    println!("streaming resumed after failover: {} insert events", insert_events(&destination).await);

    // 8. Stop the writer and let the pipeline drain, then verify no committed
    //    row was lost: source rows == rows the destination saw (copy + inserts).
    stop.store(true, Ordering::Relaxed);
    let _ = writer.await;
    let client = connect().await;
    let src = source_count(&client).await as usize;
    wait_until("consumer drains to source", Duration::from_secs(120), || async {
        copied_rows(&destination).await + insert_events(&destination).await >= src
    })
    .await;

    let copied = copied_rows(&destination).await;
    let inserts = insert_events(&destination).await;
    println!("FINAL: source={src}, destination copied={copied} + inserts={inserts} = {}", copied + inserts);
    assert_eq!(copied + inserts, src, "every committed source row reached the destination");

    pipeline.shutdown();
    let _ = client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION}; DROP TABLE IF EXISTS {TABLE} CASCADE;"
        ))
        .await;
}
