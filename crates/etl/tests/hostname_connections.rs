//! Opt-in real-network checks driven by scripts/network-tests.

use std::{
    env::var,
    error::Error,
    fs::read_to_string,
    net::IpAddr,
    time::{Duration, Instant},
};

use etl::{
    error::{ErrorKind, EtlError},
    event::{Event, EventType},
    postgres::client::PgReplicationClient,
    store::{CachedStore, PostgresStore, StateStore, WorkerType},
    test_utils::{
        database::spawn_source_database,
        event::EventCondition,
        memory_destination::MemoryDestination,
        pipeline::{create_pipeline, random_pipeline_id},
        store::wait_for_table_sync_complete,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_users_inserts, insert_users_data,
            setup_test_database_schema,
        },
    },
};
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use etl_postgres::{source::connect_to_source_database, tokio::tls::MakeRustlsConnect};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::aws_lc_rs::default_provider,
    pki_types::{CertificateDer, pem::PemObject},
};
use serde_json::{Value, from_slice, json};
use sqlx::{Error as SqlxError, query_as, query_scalar};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, lookup_host},
    spawn,
    time::{Instant as TokioInstant, pause, resume, sleep, timeout, timeout_at},
};
use tokio_postgres::{Config as TokioPgConfig, error::SqlState};
use tokio_util::task::AbortOnDropHandle;

/// Test endpoint addresses, isolated from the host network by Docker.
const ADDRESS_A: &str = "fd42:6574:6c::11";
/// Successor endpoint for the same PostgreSQL instance.
const ADDRESS_B: &str = "fd42:6574:6c::12";
/// Observer deadline; this does not alter any driver's connection timeout.
const DEADLINE: Duration = Duration::from_secs(35);
/// Errors in an opt-in fixture or driver probe.
type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

/// Applies a fixture transition and waits for the acknowledged response.
async fn control(mode: &str) -> TestResult<Value> {
    assert_eq!(var("ETL_NETWORK_LAB").as_deref(), Ok("1"));

    timeout(Duration::from_secs(5), async {
        let mut socket = TcpStream::connect("172.30.248.10:8000").await?;
        let request = if mode.is_empty() { json!({}) } else { json!({"mode": mode}) };
        socket.write_all(format!("{request}\n").as_bytes()).await?;

        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await?;

        let response: Value = from_slice(&bytes)?;
        assert!(response.get("error").is_none(), "{response}");

        Ok(response)
    })
    .await?
}

/// Waits for real resolver results, without substituting a resolver in either
/// driver.
async fn resolve(expected: &str, mixed: bool) -> TestResult<Vec<IpAddr>> {
    timeout(Duration::from_secs(5), async {
        loop {
            let addresses: Vec<_> =
                lookup_host(("source.test", 5432)).await?.map(|a| a.ip()).collect();

            if addresses.contains(&expected.parse()?)
                && addresses.iter().any(IpAddr::is_ipv4) == mixed
                && addresses.iter().filter(|a| a.is_ipv6()).all(|a| a.to_string() == expected)
            {
                return Ok(addresses);
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

/// Builds the same hostname-verifying TLS adapter used by replication.
fn tls_connector(config: &PgConnectionConfig) -> TestResult<MakeRustlsConnect> {
    let mut roots = RootCertStore::empty();

    for certificate in CertificateDer::pem_slice_iter(config.tls.trusted_root_certs.as_bytes()) {
        roots.add(certificate?)?;
    }

    let tls = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();

    Ok(MakeRustlsConnect::new(tls))
}

/// Queries the server through the same shared options and TLS adapter as
/// replication.
async fn tokio_probe(config: &PgConnectionConfig) -> TestResult<(String, i32, bool)> {
    let options: TokioPgConfig = config.with_db(None);
    let (client, connection) = options.connect(tls_connector(config)?).await?;
    let driver = AbortOnDropHandle::new(spawn(connection));

    let row = client
        .query_one(
            "select host(inet_server_addr()), pg_backend_pid(),
         (select ssl from pg_stat_ssl where pid = pg_backend_pid())",
            &[],
        )
        .await?;
    let result = (row.get(0), row.get(1), row.get(2));

    drop(client);
    driver.await??;

    Ok(result)
}

/// Opens a real SQLx pool and reports its actual server address and TLS state.
async fn sqlx_probe(config: &PgConnectionConfig) -> TestResult<(String, i32, bool)> {
    let pool = connect_to_source_database(config, 0, 1, None).await?;

    let result = query_as(
        "select host(inet_server_addr()), pg_backend_pid(),
         (select ssl from pg_stat_ssl where pid = pg_backend_pid())",
    )
    .fetch_one(&pool)
    .await;

    pool.close().await;

    Ok(result?)
}

/// Verifies copied rows, streaming, pooled replacement, and reopened persistent
/// state after DNS changes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn ipv6_pipeline_and_dns_change() -> TestResult {
    println!("NETWORK_PIPELINE initial_rules={}", control("a").await?);
    assert!(!resolve(ADDRESS_A, false).await?.is_empty());

    let mut database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table = schema.users_schema();
    insert_users_data(&mut database, &table.name, 1..=3).await;

    let mut config = database.config.clone();
    config.host = "source.test".to_owned();
    assert!(config.hostaddr.is_none());

    println!("NETWORK_PIPELINE initial_tokio=starting");
    let initial_tokio = timeout(DEADLINE, tokio_probe(&config)).await??;
    println!("NETWORK_PIPELINE initial_tokio=connected");
    assert_eq!(initial_tokio.0, ADDRESS_A);
    assert!(initial_tokio.2);

    let pool = connect_to_source_database(&config, 0, 1, None).await?;
    let initial_sqlx: (String, i32) =
        query_as("select host(inet_server_addr()), pg_backend_pid()").fetch_one(&pool).await?;
    assert_eq!(initial_sqlx.0, ADDRESS_A);

    let pipeline_id = random_pipeline_id();
    let store = PostgresStore::new(&pipeline_id, config.clone()).await?;
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline(
        &config,
        &pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await?;
    wait_for_table_sync_complete(&store, table.id, DEADLINE).await?;

    let first = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table.id, 1)])
        .await;

    insert_users_data(&mut database, &table.name, 4..=4).await;

    timeout(DEADLINE, first.notified()).await?;

    // Retire the endpoint after this transaction's durable boundary, so allowed
    // replay of unfinished work cannot masquerade as a DNS regression.
    let commit_lsn = destination
        .get_events()
        .await
        .into_iter()
        .find_map(|event| match event {
            Event::Insert(insert) => Some(insert.commit_lsn),
            _ => None,
        })
        .unwrap();

    timeout(DEADLINE, async {
        loop {
            if store
                .get_replication_checkpoint(WorkerType::Apply)
                .await?
                .is_some_and(|lsn| lsn > commit_lsn)
            {
                return Ok::<_, Box<dyn Error + Send + Sync>>(());
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    // The control socket and admin connection are independent of the retired
    // address.
    let transition = control("b").await?;
    assert!(transition["retired_backends"].as_u64().unwrap() > 0);
    println!("NETWORK_TRANSITION {transition}");

    resolve(ADDRESS_B, false).await?;

    let new_tokio = timeout(DEADLINE, tokio_probe(&config)).await??;
    assert_eq!(new_tokio.0, ADDRESS_B);
    assert_ne!(new_tokio.1, initial_tokio.1);
    assert!(new_tokio.2);

    let new_sqlx: (String, i32) = timeout(
        DEADLINE,
        query_as("select host(inet_server_addr()), pg_backend_pid()").fetch_one(&pool),
    )
    .await??;
    assert_eq!(new_sqlx.0, ADDRESS_B);
    assert_ne!(new_sqlx.1, initial_sqlx.1);

    pool.close().await;

    let after_change = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table.id, 2)])
        .await;

    insert_users_data(&mut database, &table.name, 5..=5).await;

    timeout(DEADLINE, after_change.notified()).await?;

    pipeline.shutdown_and_wait().await?;

    assert_eq!(destination.get_table_rows().await[&table.id].len(), 3);

    let events = destination.get_events().await;
    let inserts: Vec<_> = events
        .into_iter()
        .filter(|event| {
            matches!(event,
                Event::Insert(insert) if insert.replicated_table_schema.id() == table.id
            )
        })
        .collect();
    let expected = build_expected_users_inserts(4, &table, vec![("user_4", 4), ("user_5", 5)]);
    assert_events_equal(&inserts, &expected);

    let checkpoint = store.get_replication_checkpoint(WorkerType::Apply).await?;
    assert!(checkpoint.is_some());

    drop(store);
    let reopened = PostgresStore::new(&pipeline_id, config.clone()).await?;
    reopened.load_cache().await?;

    assert_eq!(reopened.get_replication_checkpoint(WorkerType::Apply).await?, checkpoint);

    // The stale pin must fail while the unchanged hostname-only configuration
    // works.
    config.hostaddr = Some(ADDRESS_A.parse()?);
    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());
    println!(
        "NETWORK_RESULT ipv6_pipeline_dns_change old={ADDRESS_A} new={ADDRESS_B} copy=3 cdc=2 \
         persistent_reload=ok"
    );

    control("a").await?;

    Ok(())
}

/// Requires hostname verification while preserving both clients' explicit-pin
/// behavior.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn tls_and_pin_controls() -> TestResult {
    control("a").await?;
    resolve(ADDRESS_A, false).await?;

    let database = spawn_source_database().await;
    let mut config = database.config.clone();
    config.host = "source.test".to_owned();

    assert!(timeout(DEADLINE, tokio_probe(&config)).await??.2);
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await??.2);
    drop(timeout(DEADLINE, PgReplicationClient::connect(config.clone())).await??);
    println!("NETWORK_TLS valid_hostname=ok");

    config.host = "wrong.test".to_owned();
    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, PgReplicationClient::connect(config.clone())).await?.is_err());

    config.host = "source.test".to_owned();
    let trusted_roots = config.tls.trusted_root_certs.clone();
    config.tls.trusted_root_certs = read_to_string("/certs/untrusted.crt")?;

    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, PgReplicationClient::connect(config.clone())).await?.is_err());

    config.tls.trusted_root_certs = trusted_roots;
    println!("NETWORK_TLS wrong_hostname=rejected untrusted_ca=rejected");

    // This hostname resolves to the admin address, so reaching A proves the
    // override is used.
    config.host = "admin.test".to_owned();
    config.hostaddr = Some(ADDRESS_A.parse()?);

    assert_eq!(timeout(DEADLINE, tokio_probe(&config)).await??.0, ADDRESS_A);
    assert_eq!(timeout(DEADLINE, sqlx_probe(&config)).await??.0, ADDRESS_A);
    println!("NETWORK_TLS ipv6_pin=ok");

    config.hostaddr = Some("172.30.248.10".parse()?);

    assert_eq!(timeout(DEADLINE, tokio_probe(&config)).await??.0, "172.30.248.10");
    assert_eq!(timeout(DEADLINE, sqlx_probe(&config)).await??.0, "172.30.248.10");
    drop(timeout(DEADLINE, PgReplicationClient::connect(config.clone())).await??);
    println!("NETWORK_TLS ipv4_pin=ok");

    control("drop6").await?;
    resolve(ADDRESS_A, true).await?;
    config.hostaddr = None;
    config.host = "wrong.test".to_owned();

    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, PgReplicationClient::connect(config)).await?.is_err());

    control("a").await?;
    println!("NETWORK_RESULT tls valid_host=ok wrong_host=rejected explicit_pin=ok");

    Ok(())
}

/// A nonresponsive DNS server must not leave replication or a pool acquisition
/// waiting beyond the connection budget.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn stalled_dns_is_bounded() -> TestResult {
    control("a").await?;

    let database = spawn_source_database().await;
    let mut config = database.config.clone();
    config.host = "source.test".to_owned();

    control("drop_dns").await?;

    let start = Instant::now();
    let replication = timeout(DEADLINE, PgReplicationClient::connect(config.clone())).await;
    let replication_ms = start.elapsed().as_millis();

    let start = Instant::now();
    let pool = timeout(DEADLINE, sqlx_probe(&config)).await;
    let pool_ms = start.elapsed().as_millis();

    let rules = control("").await?;
    control("a").await?;

    assert!(replication?.is_err());
    assert!(pool?.is_err());
    assert!(replication_ms < 32_000 && pool_ms < 32_000);

    println!(
        "NETWORK_RESULT stalled_dns replication_ms={replication_ms} pool_ms={pool_ms} \
         rules={rules}"
    );

    Ok(())
}

/// Exhausted addresses and missing DNS must return errors within the existing
/// pool budget; an explicit pin cannot escape through another DNS address.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn unreachable_and_missing_dns_are_bounded() -> TestResult {
    control("a").await?;

    let database = spawn_source_database().await;
    let mut config = database.config.clone();
    config.host = "source.test".to_owned();

    control("drop_all").await?;
    resolve(ADDRESS_A, true).await?;

    let start = Instant::now();
    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(start.elapsed() < Duration::from_secs(12));

    let start = Instant::now();
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());
    assert!(start.elapsed() < Duration::from_secs(32));

    println!("NETWORK_RESULT all_unreachable=bounded rules={}", control("").await?);

    control("drop4").await?;
    config.hostaddr = Some("172.30.248.10".parse()?);

    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());

    control("a").await?;
    config.hostaddr = None;
    config.host = "missing.test".to_owned();

    assert!(timeout(DEADLINE, tokio_probe(&config)).await?.is_err());
    assert!(timeout(DEADLINE, sqlx_probe(&config)).await?.is_err());

    println!("NETWORK_RESULT unreachable_pin=rejected missing_dns=rejected");

    Ok(())
}

/// Accepted sockets stalled in TLS or authentication must hit the connection
/// budget and close without waiting for an operating-system TCP timeout.
#[tokio::test]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn stalled_handshakes_time_out_and_close_sockets() -> TestResult {
    control("a").await?;
    let _ = default_provider().install_default();

    for driver in ["replication_tls", "sqlx_tls", "sqlx_authentication"] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let tls_enabled = driver != "sqlx_authentication";
        let config = PgConnectionConfig {
            host: "source.test".to_owned(),
            hostaddr: Some("127.0.0.1".parse()?),
            port: listener.local_addr()?.port(),
            name: "unused".to_owned(),
            username: "unused".to_owned(),
            password: None,
            tls: TlsConfig {
                enabled: tls_enabled,
                trusted_root_certs: read_to_string("/certs/root.crt")?,
            },
            keepalive: TcpKeepaliveConfig::default(),
        };

        // Keep the observer independent, with one second of scheduling
        // tolerance.
        let deadline = TokioInstant::now() + Duration::from_secs(31);
        let connection = AbortOnDropHandle::new(spawn(async move {
            if driver == "replication_tls" {
                PgReplicationClient::connect(config)
                    .await
                    .map(drop)
                    .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            } else {
                sqlx_probe(&config).await.map(drop)
            }
        }));

        let (mut socket, _) = timeout(Duration::from_secs(5), listener.accept()).await??;
        timeout(Duration::from_secs(5), async {
            let length = socket.read_u32().await?;
            let mut startup = vec![0; usize::try_from(length - 4)?];
            socket.read_exact(&mut startup).await?;
            assert_eq!(startup, [4, 210, 22, 47]);

            if tls_enabled {
                socket.write_all(b"S").await?;
                let mut client_hello = [0; 4096];
                assert!(socket.read(&mut client_hello).await? > 0);
            } else {
                // SQLx prefers TLS; reject it and observe protocol-3.0 startup
                // before stalling authentication.
                socket.write_all(b"N").await?;
                let length = socket.read_u32().await?;
                startup.resize(usize::try_from(length - 4)?, 0);
                socket.read_exact(&mut startup).await?;
                assert!(startup.starts_with(&[0, 3, 0, 0]));
            }

            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .await??;

        pause();
        let result = timeout_at(deadline, connection).await;
        resume();

        let error = result??.unwrap_err();

        if driver == "replication_tls" {
            assert_eq!(
                error.downcast_ref::<EtlError>().unwrap().kind(),
                ErrorKind::SourceConnectionFailed
            );
        } else {
            assert!(matches!(error.downcast_ref::<SqlxError>(), Some(SqlxError::PoolTimedOut)));
        }

        let mut remaining = Vec::new();
        timeout(Duration::from_secs(5), socket.read_to_end(&mut remaining)).await??;

        println!("NETWORK_RESULT stalled_handshake driver={driver} result=timed_out socket=closed");
    }

    Ok(())
}

/// Query cancellation must reuse the winning address after a family fallback.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn cancellation_targets_the_winning_address() -> TestResult {
    control("a").await?;

    let database = spawn_source_database().await;
    let admin = connect_to_source_database(&database.config, 0, 1, None).await?;

    let mut config = database.config.clone();
    config.host = "source.test".to_owned();

    control("drop6").await?;
    resolve(ADDRESS_A, true).await?;

    let options: TokioPgConfig = config.with_db(None);
    let (client, connection) = options.connect(tls_connector(&config)?).await?;
    let driver = AbortOnDropHandle::new(spawn(connection));

    let row = client.query_one("select pg_backend_pid(), host(inet_server_addr())", &[]).await?;
    let pid: i32 = row.get(0);

    assert_eq!(row.get::<_, String>(1), "172.30.248.10");

    let cancellation = client.cancel_token();
    let query =
        AbortOnDropHandle::new(spawn(
            async move { client.simple_query("select pg_sleep(60)").await },
        ));

    timeout(Duration::from_secs(5), async {
        loop {
            let wait_event: Option<String> =
                query_scalar("select wait_event from pg_stat_activity where pid = $1")
                    .bind(pid)
                    .fetch_one(&admin)
                    .await?;

            if wait_event.as_deref() == Some("PgSleep") {
                return Ok::<_, SqlxError>(());
            }

            sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    timeout(Duration::from_secs(2), cancellation.cancel_query(tls_connector(&config)?)).await??;

    let error = timeout(Duration::from_secs(2), query).await??.unwrap_err();

    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED));

    driver.await??;
    admin.close().await;
    control("a").await?;

    println!("NETWORK_RESULT query_cancellation=winning_address");

    Ok(())
}

/// Requires fast fallback with either address family healthy or unreachable.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated scripts/network-tests fixture"]
async fn mixed_address_fallback() -> TestResult {
    control("a").await?;

    let database = spawn_source_database().await;
    let mut config = database.config.clone();
    config.host = "source.test".to_owned();

    let mut failures = Vec::new();

    for mode in ["v4", "mixed", "reject4", "drop4", "reject6", "drop6"] {
        control(mode).await?;
        let expected = if mode == "v4" { "172.30.248.10" } else { ADDRESS_A };
        let addresses = resolve(expected, true).await?;

        if mode != "v4" {
            assert_eq!(addresses[0].is_ipv4(), var("NETWORK_IPV4_FIRST").as_deref() == Ok("1"));
        }

        let start = Instant::now();
        let raw = timeout(DEADLINE, tokio_probe(&config)).await;
        let raw_ms = start.elapsed().as_millis();

        let start = Instant::now();
        let sqlx = timeout(DEADLINE, sqlx_probe(&config)).await;
        let sqlx_ms = start.elapsed().as_millis();

        let raw_ok = matches!(&raw, Ok(Ok(_)));
        let sqlx_ok = matches!(&sqlx, Ok(Ok(_)));

        for observation in [&raw, &sqlx] {
            if let Ok(Ok((endpoint, _, encrypted))) = observation {
                assert!(encrypted);

                if mode != "mixed" {
                    let expected = if matches!(mode, "v4" | "reject6" | "drop6") {
                        "172.30.248.10"
                    } else {
                        ADDRESS_A
                    };

                    assert_eq!(endpoint, expected);
                }
            }
        }

        let raw_endpoint = raw.as_ref().ok().and_then(|r| r.as_ref().ok()).map(|r| &r.0);
        let sqlx_endpoint = sqlx.as_ref().ok().and_then(|r| r.as_ref().ok()).map(|r| &r.0);
        println!(
            "NETWORK_RESULT mode={mode} addresses={addresses:?} tokio_ok={raw_ok} \
             tokio_endpoint={raw_endpoint:?} tokio_deadline={} tokio_ms={raw_ms} \
             sqlx_ok={sqlx_ok} sqlx_endpoint={sqlx_endpoint:?} sqlx_deadline={} sqlx_ms={sqlx_ms}",
            raw.is_err(),
            sqlx.is_err()
        );
        println!("NETWORK_RULES {}", control("").await?);

        if !raw_ok || !sqlx_ok || raw_ms >= 2_000 || sqlx_ms >= 2_000 {
            failures.push(mode);
        }
    }

    control("a").await?;

    assert!(failures.is_empty(), "Connection failures: {failures:?}");

    Ok(())
}
