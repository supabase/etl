use std::time::Duration;

use anyhow::Context;
use etl::replication::{scope as postgres_capture, scope::*};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

fn source_relation() -> Relation {
    Relation {
        oid: 42,
        namespace: "public".into(),
        name: "accounts".into(),
        replica_identity: b'd',
        columns: vec![Column { key: true, name: "id".into(), type_oid: 20, type_modifier: -1 }],
    }
}

#[test]
fn scope_capture_rejects_binary_lengths_before_allocation() -> anyhow::Result<()> {
    let mut decoder =
        Decoder::new("selected_publication".to_owned(), vec![source_relation()], 8 * 1024 * 1024)?;
    let mut insert = vec![b'I'];
    insert.extend_from_slice(&42u32.to_be_bytes());
    insert.extend_from_slice(&[b'N', 0, 1, b'b']);
    insert.extend_from_slice(&i32::MAX.to_be_bytes());
    let error = decoder.push(&insert).err().context("unnegotiated binary output was accepted")?;
    assert!(format!("{error:#}").contains("binary tuple output was not negotiated"));
    Ok(())
}

#[test]
fn scope_capture_rejects_allocation_counts_before_protocol_parsing() -> anyhow::Result<()> {
    let mut relation = vec![b'R', 0, 0, 0, 42];
    relation.extend_from_slice(b"public\0accounts\0d\xff\xff");
    let malformed = [
        relation,
        vec![b'T', 0x7f, 0xff, 0xff, 0xff, 0],
        vec![b'T', 0xff, 0xff, 0xff, 0xff, 0],
        vec![b'I', 0, 0, 0, 42, b'N', 0xff, 0xff],
    ];
    for message in malformed {
        let mut decoder = Decoder::new(
            "selected_publication".to_owned(),
            vec![source_relation()],
            8 * 1024 * 1024,
        )?;
        assert!(decoder.push(&message).is_err());
    }
    Ok(())
}

#[test]
fn scope_capture_seals_only_the_matching_commit() -> anyhow::Result<()> {
    let mut decoder =
        Decoder::new("selected_publication".to_owned(), vec![source_relation()], 8 * 1024 * 1024)?;
    let mut begin = vec![b'B'];
    begin.extend_from_slice(&9007199254740993u64.to_be_bytes());
    begin.extend_from_slice(&123i64.to_be_bytes());
    begin.extend_from_slice(&7u32.to_be_bytes());
    assert!(decoder.push(&begin)?.is_none());
    assert!(!decoder.is_idle());
    let mut commit = vec![b'C', 0];
    commit.extend_from_slice(&9007199254740993u64.to_be_bytes());
    commit.extend_from_slice(&9007199254740994u64.to_be_bytes());
    commit.extend_from_slice(&123i64.to_be_bytes());
    let transaction = decoder.push(&commit)?.context("complete transaction was not sealed")?;
    assert_eq!(transaction.end_lsn, "9007199254740994");
    assert!(decoder.is_idle());
    assert!(
        decoder.push(&commit).is_err(),
        "a repeated commit without its transaction must not create input"
    );
    Ok(())
}

fn logical_message(flags: u8, position: u64, prefix: &str, content: &[u8]) -> Vec<u8> {
    let mut message = vec![b'M', flags];
    message.extend_from_slice(&position.to_be_bytes());
    message.extend_from_slice(prefix.as_bytes());
    message.push(0);
    message.extend_from_slice(&(content.len() as i32).to_be_bytes());
    message.extend_from_slice(content);
    message
}

#[test]
fn scope_capture_refuses_selected_table_ddl_before_a_commit() -> anyhow::Result<()> {
    let mut decoder =
        Decoder::new("selected_publication".to_owned(), vec![source_relation()], 8 * 1024 * 1024)?;
    let mut begin = vec![b'B'];
    begin.extend_from_slice(&256u64.to_be_bytes());
    begin.extend_from_slice(&123i64.to_be_bytes());
    begin.extend_from_slice(&7u32.to_be_bytes());
    decoder.push(&begin)?;
    let ddl = serde_json::json!({
        "command_tag": "ALTER TABLE", "nspname": "public", "relname": "accounts", "oid": 42,
        "identity": {"primary_key_attnums": [1], "relreplident": "d", "replica_identity_index_attnums": []}, "columns": []
    });
    let error = decoder
        .push(&logical_message(1, 240, "supabase_etl_ddl", &serde_json::to_vec(&ddl)?))
        .err()
        .context("selected-table DDL was accepted")?;
    assert!(matches!(error.downcast_ref::<CaptureRefusal>(), Some(CaptureRefusal::SchemaChanged)));
    Ok(())
}

#[test]
fn scope_capture_scopes_shared_table_publication_changes() -> anyhow::Result<()> {
    for (command, publication, refused) in [
        ("ALTER PUBLICATION", Some("other"), false),
        ("ALTER PUBLICATION", Some("selected_publication"), true),
        ("ALTER PUBLICATION", None, false),
        ("ALTER TABLE", None, true),
    ] {
        let mut decoder = Decoder::new(
            "selected_publication".to_owned(),
            vec![source_relation()],
            8 * 1024 * 1024,
        )?;
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&256u64.to_be_bytes());
        begin.extend_from_slice(&123i64.to_be_bytes());
        begin.extend_from_slice(&7u32.to_be_bytes());
        decoder.push(&begin)?;
        // The physical table is shared; only ALTER PUBLICATION is scoped.
        let ddl = serde_json::json!({
            "command_tag": command, "publication_name": publication,
            "nspname": "public", "relname": "accounts", "oid": 42,
            "identity": {"primary_key_attnums": [1], "relreplident": "d", "replica_identity_index_attnums": []},
            "columns": []
        });
        let result =
            decoder.push(&logical_message(1, 240, "supabase_etl_ddl", &serde_json::to_vec(&ddl)?));
        if refused {
            assert!(matches!(
                result.unwrap_err().downcast_ref::<CaptureRefusal>(),
                Some(CaptureRefusal::SchemaChanged),
            ));
        } else {
            assert!(result?.is_none());
            let mut commit = vec![b'C', 0];
            commit.extend_from_slice(&256u64.to_be_bytes());
            commit.extend_from_slice(&257u64.to_be_bytes());
            commit.extend_from_slice(&123i64.to_be_bytes());
            assert!(decoder.push(&commit)?.is_some());
        }
    }
    Ok(())
}

async fn authenticate(server: &mut DuplexStream) -> anyhow::Result<()> {
    let length = server.read_u32().await?;
    anyhow::ensure!((8..=4096).contains(&length), "invalid test startup length");
    let mut startup = vec![0; length as usize - 4];
    server.read_exact(&mut startup).await?;
    // AuthenticationOk followed by ReadyForQuery, entirely in memory.
    server.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'Z', 0, 0, 0, 5, b'I']).await?;
    Ok(())
}

async fn response(server: &mut DuplexStream, tag: u8, payload: &[u8]) -> anyhow::Result<()> {
    server.write_u8(tag).await?;
    server.write_u32(u32::try_from(payload.len() + 4)?).await?;
    server.write_all(payload).await?;
    Ok(())
}

async fn query_rows(server: &mut DuplexStream, rows: &[Vec<&str>]) -> anyhow::Result<()> {
    if let Some(first) = rows.first() {
        let mut description = Vec::new();
        description.extend_from_slice(&u16::try_from(first.len())?.to_be_bytes());
        for _ in first {
            description.extend_from_slice(b"column\0");
            description.extend_from_slice(&0u32.to_be_bytes());
            description.extend_from_slice(&0i16.to_be_bytes());
            description.extend_from_slice(&25u32.to_be_bytes());
            description.extend_from_slice(&(-1i16).to_be_bytes());
            description.extend_from_slice(&(-1i32).to_be_bytes());
            description.extend_from_slice(&0i16.to_be_bytes());
        }
        response(server, b'T', &description).await?;
        for row in rows {
            let mut payload = Vec::new();
            payload.extend_from_slice(&u16::try_from(row.len())?.to_be_bytes());
            for value in row {
                payload.extend_from_slice(&i32::try_from(value.len())?.to_be_bytes());
                payload.extend_from_slice(value.as_bytes());
            }
            response(server, b'D', &payload).await?;
        }
    }
    response(server, b'C', b"SELECT 0\0").await?;
    response(server, b'Z', b"I").await
}

async fn snapshot_connection(
    snapshot: &'static str,
) -> anyhow::Result<(scope::Connection, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let (client, mut server) = tokio::io::duplex(8192);
    let peer = tokio::spawn(async move {
        authenticate(&mut server).await?;
        loop {
            let tag = match server.read_u8().await {
                Ok(tag) => tag,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let length = server.read_u32().await?;
            anyhow::ensure!((4..=8192).contains(&length), "test query exceeds bound");
            let mut query = vec![0; length as usize - 4];
            server.read_exact(&mut query).await?;
            if tag == b'X' {
                return Ok(());
            }
            anyhow::ensure!(tag == b'Q', "unexpected test query protocol");
            if query.starts_with(b"CREATE_REPLICATION_SLOT") {
                query_rows(&mut server, &[vec!["owned", "0/100", snapshot, "pgoutput"]]).await?;
            } else if query.starts_with(b"SELECT pg_catalog.pg_logical_emit_message") {
                query_rows(&mut server, &[vec!["0/200"]]).await?;
            } else {
                query_rows(&mut server, &[]).await?;
            }
        }
    });
    let connection = scope::Connection::connect(
        scope::ConnectionOptions {
            user: "reader".into(),
            password: None,
            database: "accounts".into(),
            replication: true,
        },
        Box::new(client),
        scope::Limits { message_bytes: 4096, query_rows: 8, io_timeout: Duration::from_secs(10) },
    )
    .await?;
    Ok((connection, peer))
}

#[tokio::test]
async fn scope_snapshot_requires_every_partition_on_the_same_exported_cut() -> anyhow::Result<()> {
    use scope::snapshot::{self, Cursor, Partition, Selection};
    let (mut exporter, exporter_peer) = snapshot_connection("00000001-00000002-1").await?;
    let (mut guard, guard_peer) = snapshot_connection("00000001-00000002-1").await?;
    let (mut left, left_peer) = snapshot_connection("00000001-00000002-1").await?;
    let (mut right, right_peer) = snapshot_connection("00000001-00000002-1").await?;
    let relation = source_relation();
    let mut other = relation.clone();
    other.oid = 43;
    other.name = "transfers".into();
    assert_eq!(
        snapshot::begin(
            &mut exporter,
            &mut guard,
            &mut [&mut left, &mut right],
            "owned",
            &[relation.clone(), other.clone()]
        )
        .await?,
        256
    );
    let mut left = Cursor::new(left, 8)?;
    let mut right = Cursor::new(right, 8)?;
    left.select(&relation, &Selection::All).await?;
    right.select(&other, &Selection::All).await?;
    assert!(left.next().await?.is_none());
    assert!(right.next().await?.is_none());
    let left_seal = left.finish().await?;
    let right_seal = right.finish().await?;
    let plan = [
        Partition { relation: 42, selection: Selection::All },
        Partition { relation: 43, selection: Selection::All },
    ];
    assert!(
        snapshot::seal_scope(&plan, std::slice::from_ref(&left_seal)).is_err(),
        "a missing empty table must not become complete"
    );
    assert!(
        snapshot::seal_scope(&plan[..1], std::slice::from_ref(&left_seal)).is_err(),
        "a caller cannot shrink the declared source scope"
    );
    assert_eq!(snapshot::seal_scope(&plan, &[left_seal.clone(), right_seal.clone()])?, 256);
    assert!(
        snapshot::seal_scope(&plan, &[left_seal.clone(), left_seal, right_seal]).is_err(),
        "duplicate partitions must not establish completeness"
    );
    drop(left);
    drop(right);
    drop(exporter);
    drop(guard);
    exporter_peer.await??;
    guard_peer.await??;
    left_peer.await??;
    right_peer.await??;
    Ok(())
}

#[tokio::test]
async fn scope_transport_rejects_oversized_frames_and_owns_driver_lifetime() -> anyhow::Result<()> {
    let (client, mut server) = tokio::io::duplex(8192);
    let connect = scope::Connection::connect(
        scope::ConnectionOptions {
            user: "reader".into(),
            password: None,
            database: "accounts".into(),
            replication: false,
        },
        Box::new(client),
        scope::Limits { message_bytes: 512, query_rows: 8, io_timeout: Duration::from_secs(10) },
    );
    let (connection, authenticated) = tokio::join!(connect, authenticate(&mut server));
    authenticated?;
    let mut connection = connection?;
    // No payload is sent: the size field alone must terminate the driver.
    server.write_all(&[b'D', 0x7f, 0xff, 0xff, 0xff]).await?;
    let error = connection.query("SELECT 1").await.err().context("oversized frame was accepted")?;
    assert!(matches!(
        error.downcast_ref::<scope::CaptureRefusal>(),
        Some(scope::CaptureRefusal::Capacity)
    ));
    drop(connection);
    let mut data = [0; 256];
    // The queued query may precede closure. Drain it and observe that dropping
    // the connection owner closes its driver and underlying stream.
    while server.read(&mut data).await? != 0 {}
    Ok(())
}

#[tokio::test]
async fn scope_capture_qualifies_only_its_exact_wal_barrier() -> anyhow::Result<()> {
    let (mut sql, peer) = snapshot_connection("00000001-00000002-1").await?;
    let mut decoder =
        scope::Decoder::new("selected_publication".to_owned(), vec![source_relation()], 8192)?;
    let token = "a".repeat(64);
    decoder.request_progress(&mut sql, &token).await?;
    // An unrelated message is ignored; a mismatched token cannot consume the
    // outstanding marker or seal source progress.
    assert!(decoder.push(&logical_message(0, 512, "etl_scope_barrier", b"unrelated"))?.is_none());
    assert!(decoder.is_idle());
    assert!(decoder.take_progress().is_none());
    decoder.push(&logical_message(0, 511, "etl_scope_barrier", token.as_bytes()))?;
    assert!(decoder.take_progress().is_none());
    decoder.push(&logical_message(0, 512, "etl_scope_barrier", token.as_bytes()))?;
    assert_eq!(decoder.take_progress(), Some(512));
    assert!(decoder.take_progress().is_none());
    drop(sql);
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn scope_capture_loop_keeps_physical_wal_heads_out_of_feedback() -> anyhow::Result<()> {
    let (client, mut server) = tokio::io::duplex(8192);
    let token = "a".repeat(64);
    let expected_token = token.clone();
    let peer = tokio::spawn(async move {
        authenticate(&mut server).await?;
        assert_eq!(server.read_u8().await?, b'Q');
        let length = server.read_u32().await?;
        let mut command = vec![0; length as usize - 4];
        server.read_exact(&mut command).await?;
        assert!(command.starts_with(br#"START_REPLICATION SLOT "owned""#));
        response(&mut server, b'W', &[0, 0, 0]).await?;
        let mut keepalive = vec![b'k'];
        keepalive.extend_from_slice(&9000u64.to_be_bytes());
        keepalive.extend_from_slice(&0i64.to_be_bytes());
        keepalive.push(1);
        response(&mut server, b'd', &keepalive).await?;
        assert_eq!(server.read_u8().await?, b'd');
        assert_eq!(server.read_u32().await?, 38);
        let mut feedback = [0u8; 34];
        server.read_exact(&mut feedback).await?;
        assert_eq!(feedback[0], b'r');
        for range in [1..9, 9..17, 17..25] {
            assert_eq!(&feedback[range], &256u64.to_be_bytes());
        }
        let mut wal = vec![b'w'];
        wal.extend_from_slice(&512u64.to_be_bytes());
        wal.extend_from_slice(&9000u64.to_be_bytes());
        wal.extend_from_slice(&0i64.to_be_bytes());
        wal.extend(logical_message(0, 512, "etl_scope_barrier", expected_token.as_bytes()));
        response(&mut server, b'd', &wal).await?;
        anyhow::Ok(())
    });
    let mut connection = Connection::connect(
        ConnectionOptions {
            user: "reader".into(),
            password: None,
            database: "accounts".into(),
            replication: true,
        },
        Box::new(client),
        Limits { message_bytes: 4096, query_rows: 8, io_timeout: Duration::from_secs(10) },
    )
    .await?;
    connection.start_replication("owned", "selected_publication", 256).await?;
    let (mut sql, sql_peer) = snapshot_connection("00000001-00000002-1").await?;
    let mut decoder = Decoder::new("selected_publication".into(), vec![source_relation()], 8192)?;
    let checks = std::sync::atomic::AtomicUsize::new(0);
    let unit = capture(&mut connection, &mut sql, &mut decoder, 256, &token, || async {
        checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
    .await?;
    assert!(matches!(unit, CaptureUnit::Progress(512)));
    assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 2);
    drop(connection);
    drop(sql);
    peer.await??;
    sql_peer.await??;
    Ok(())
}

#[tokio::test]
async fn scope_capture_loop_preserves_schema_refusals() -> anyhow::Result<()> {
    let (client, mut server) = tokio::io::duplex(8192);
    let peer = tokio::spawn(async move {
        authenticate(&mut server).await?;
        assert_eq!(server.read_u8().await?, b'Q');
        let length = server.read_u32().await?;
        let mut command = vec![0; length as usize - 4];
        server.read_exact(&mut command).await?;
        response(&mut server, b'W', &[0, 0, 0]).await?;
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&256u64.to_be_bytes());
        begin.extend_from_slice(&123i64.to_be_bytes());
        begin.extend_from_slice(&7u32.to_be_bytes());
        let ddl = serde_json::json!({
            "command_tag": "ALTER TABLE", "nspname": "public", "relname": "accounts", "oid": 42,
            "identity": {"primary_key_attnums": [1], "relreplident": "d", "replica_identity_index_attnums": []}, "columns": []
        });
        for message in
            [begin, logical_message(1, 240, "supabase_etl_ddl", &serde_json::to_vec(&ddl)?)]
        {
            let mut wal = vec![b'w'];
            wal.extend_from_slice(&128u64.to_be_bytes());
            wal.extend_from_slice(&9000u64.to_be_bytes());
            wal.extend_from_slice(&0i64.to_be_bytes());
            wal.extend(message);
            response(&mut server, b'd', &wal).await?;
        }
        anyhow::Ok(())
    });
    let mut connection = Connection::connect(
        ConnectionOptions {
            user: "reader".into(),
            password: None,
            database: "accounts".into(),
            replication: true,
        },
        Box::new(client),
        Limits { message_bytes: 4096, query_rows: 8, io_timeout: Duration::from_secs(10) },
    )
    .await?;
    connection.start_replication("owned", "selected_publication", 128).await?;
    let (mut sql, sql_peer) = snapshot_connection("00000001-00000002-1").await?;
    let mut decoder = Decoder::new("selected_publication".into(), vec![source_relation()], 8192)?;
    let error =
        capture(&mut connection, &mut sql, &mut decoder, 128, &"a".repeat(64), || async { Ok(()) })
            .await
            .err()
            .context("selected-table DDL was captured")?;
    assert!(matches!(error.downcast_ref::<CaptureRefusal>(), Some(CaptureRefusal::SchemaChanged)));
    drop(connection);
    drop(sql);
    peer.await??;
    sql_peer.await??;
    Ok(())
}
