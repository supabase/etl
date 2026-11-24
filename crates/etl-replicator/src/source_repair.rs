//! Best-effort reporting of a managed source dial failure. Kubernetes owns
//! replacement.

use std::{env, time::Duration};

use etl::error::EtlError;
use etl_config::shared::validate_supabase_project_ref;
use reqwest::StatusCode;
use serde::Deserialize;
#[cfg(feature = "any-destination")]
use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until, timeout_at};
use tracing::{info, warn};

use crate::management_api::ManagementApiClient;

const SOURCE_ID_ENV: &str = "ETL_SOURCE_REPAIR_SOURCE_ID";
const CLUSTER_ID_ENV: &str = "ETL_SOURCE_REPAIR_CLUSTER_ID";
const ATTEMPT_WINDOW: Duration = Duration::from_secs(60);
const MAX_ACK_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Save,
    Rollout,
}

impl Phase {
    fn query(self) -> &'static str {
        match self {
            Self::Save => "save",
            Self::Rollout => "rollout",
        }
    }

    fn acknowledgement(self) -> &'static str {
        match self {
            Self::Save => "source_saved",
            Self::Rollout => "rollout_requested",
        }
    }
}

enum Reply {
    Acknowledged,
    Retry,
    Stop,
}

#[derive(Deserialize)]
struct Acknowledgement<'a> {
    status: &'a str,
}

/// One owner polls the reporter alongside startup or replication. Dropping its
/// future cancels the current request or timer without detaching work.
pub(crate) struct SourceRepair<'a> {
    api: &'a ManagementApiClient,
    source_id: String,
    cluster_id: String,
}

impl<'a> SourceRepair<'a> {
    /// The managed pod opts in by supplying both routing IDs at startup.
    pub(crate) fn from_env(api: &'a ManagementApiClient) -> Option<Self> {
        Self::new(api, env::var(SOURCE_ID_ENV).ok(), env::var(CLUSTER_ID_ENV).ok())
    }

    fn new(
        api: &'a ManagementApiClient,
        source_id: Option<String>,
        cluster_id: Option<String>,
    ) -> Option<Self> {
        let source_id = source_id?;
        let cluster_id = cluster_id?;
        validate_supabase_project_ref(api.project_ref()).ok()?;
        if source_id.is_empty()
            || !source_id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            || cluster_id.trim().is_empty()
        {
            return None;
        }
        Some(Self { api, source_id, cluster_id })
    }

    /// Gives an eligible startup error one bounded opportunity for repair and
    /// replacement before returning the original error to the supervisor.
    pub(crate) async fn startup(&self, error: &EtlError) {
        if error.is_postgres_dial_failure() {
            let deadline = Instant::now() + ATTEMPT_WINDOW;
            if matches!(timeout_at(deadline, self.report()).await, Ok(true)) {
                sleep_until(deadline).await;
            }
        }
    }

    /// Coalesces worker failures and requires a fresh observation after
    /// cooldown before starting another save/rollout sequence.
    #[cfg(feature = "any-destination")]
    pub(crate) async fn run(&self, mut failures: watch::Receiver<()>) {
        while failures.changed().await.is_ok() {
            let _ = timeout_at(Instant::now() + ATTEMPT_WINDOW, self.report()).await;
            sleep(Duration::from_secs(rand::random_range(60..=90))).await;
            failures.borrow_and_update();
        }
    }

    async fn report(&self) -> bool {
        for phase in [Phase::Save, Phase::Rollout] {
            let mut backoff_seconds = 2;
            loop {
                match self.request(phase).await {
                    Reply::Acknowledged => break,
                    Reply::Stop => return false,
                    Reply::Retry => {
                        warn!(phase = phase.query(), "retrying unconfirmed managed source repair");
                        let delay = rand::random_range(backoff_seconds..=2 * backoff_seconds);
                        sleep(Duration::from_secs(delay)).await;
                        backoff_seconds = (2 * backoff_seconds).min(8);
                    }
                }
            }
        }
        info!("managed source rollout requested");
        true
    }

    async fn request(&self, phase: Phase) -> Reply {
        let Ok(mut response) =
            self.api.request_source_repair(&self.source_id, &self.cluster_id, phase.query()).await
        else {
            return Reply::Retry;
        };
        let status = response.status();
        if status != StatusCode::OK {
            warn!(phase = phase.query(), %status, "managed source repair request declined");
            return if status.is_server_error()
                || matches!(status, StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS)
            {
                Reply::Retry
            } else {
                Reply::Stop
            };
        }
        let mut body = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if chunk.len() <= MAX_ACK_BYTES - body.len() => {
                    body.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Ok(Some(_)) => return Reply::Stop,
                Err(_) => return Reply::Retry,
            }
        }
        match serde_json::from_slice::<Acknowledgement>(&body) {
            Ok(ack) if ack.status == phase.acknowledgement() => Reply::Acknowledged,
            Err(error) if error.is_eof() => Reply::Retry,
            _ => {
                warn!(phase = phase.query(), "unexpected managed source repair acknowledgement");
                Reply::Stop
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use etl::{error::ErrorKind, migrations::run_source_migrations};
    use etl_config::shared::ReplicatorConfig;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        time::{advance, pause, timeout},
    };

    use super::*;

    const PROJECT: &str = "abcdefghijklmnopqrst";

    fn unreachable_config() -> ReplicatorConfig {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        serde_json::from_value(serde_json::json!({
            "pipeline": {"id": "1", "publication_name": "test", "pg_connection": {
                "host": "127.0.0.1", "port": port, "name": "test", "username": "test",
                "tls": {"enabled": false, "trusted_root_certs": ""}
            }},
            "destination": {"ducklake": {"catalog_url": "ducklake:postgres:host=example.com", "data_path": "s3://example-bucket/data"}}
        })).unwrap()
    }

    async fn server() -> (TcpListener, ManagementApiClient) {
        let listener = TcpListener::bind("[::1]:0").await.unwrap();
        let api = ManagementApiClient::new(
            format!("http://{}", listener.local_addr().unwrap()),
            "fake-system-key".into(),
            PROJECT.into(),
            "1".into(),
        )
        .unwrap();
        (listener, api)
    }

    fn reporter(api: &ManagementApiClient) -> SourceRepair<'_> {
        SourceRepair::new(api, Some("42".into()), Some("cluster-a".into())).unwrap()
    }

    /// Reading the full request before dropping the socket models a lost
    /// acknowledgement.
    async fn receive(listener: &TcpListener, phase: &str) -> TcpStream {
        let (socket, _) =
            timeout(Duration::from_secs(8), listener.accept()).await.unwrap().unwrap();
        let mut stream = BufReader::new(socket);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        assert_eq!(
            line,
            format!(
                "POST /system/replication/{PROJECT}/sources/42?cluster_id=cluster-a&\
                 repair_phase={phase} HTTP/1.1\r\n"
            )
        );
        let mut key = false;
        loop {
            line.clear();
            assert!(stream.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                break;
            }
            let header = line.trim_end().to_ascii_lowercase();
            if header.starts_with("apikey:") {
                assert_eq!(header, "apikey: fake-system-key");
                key = true;
            }
            assert!(!header.starts_with("transfer-encoding:"));
            if header.starts_with("content-length:") {
                assert_eq!(header, "content-length: 0");
            }
        }
        assert!(key);
        stream.into_inner()
    }

    async fn reply(mut socket: TcpStream, body: &str) -> TcpStream {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket
    }

    async fn acknowledge_save(listener: &TcpListener) {
        reply(receive(listener, "save").await, r#"{"status":"source_saved"}"#).await;
    }

    async fn acknowledge_rollout(listener: &TcpListener) -> TcpStream {
        reply(receive(listener, "rollout").await, r#"{"status":"rollout_requested"}"#).await
    }

    #[tokio::test]
    async fn retries_each_lost_reply_before_advancing_to_the_next_phase() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        let (confirmed, ()) = tokio::join!(repair.report(), async {
            drop(receive(&listener, "save").await);
            acknowledge_save(&listener).await;
            drop(receive(&listener, "rollout").await);
            acknowledge_rollout(&listener).await;
        });
        assert!(confirmed);
    }

    #[tokio::test]
    async fn rejects_wrong_or_oversized_save_acknowledgements() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        for body in [r#"{"status":"rollout_requested"}"#.to_owned(), " ".repeat(MAX_ACK_BYTES + 1)]
        {
            let (confirmed, ()) = tokio::join!(repair.report(), async {
                reply(receive(&listener, "save").await, &body).await;
            });
            assert!(!confirmed);
        }
    }

    #[tokio::test]
    async fn replacement_resumes_after_the_first_caller_disappears() {
        let (listener, api) = server().await;
        let first = reporter(&api);
        tokio::select! {
            _ = first.report() => panic!("first caller completed before save acknowledgement"),

            // The backend committed this request, then the pod was replaced.
            socket = receive(&listener, "save") => drop(socket),
        }
        let replacement = reporter(&api);
        let (confirmed, ()) = tokio::join!(replacement.report(), async {
            acknowledge_save(&listener).await;
            acknowledge_rollout(&listener).await;
        });
        assert!(confirmed);
    }

    #[tokio::test]
    async fn incomplete_routing_and_unrelated_errors_do_not_report() {
        let (listener, api) = server().await;
        for (source, cluster) in [
            (None, None),
            (Some("42"), None),
            (None, Some("cluster-a")),
            (Some(""), Some("cluster-a")),
            (Some("42"), Some(" ")),
            (Some("../emails"), Some("cluster-a")),
        ] {
            assert!(
                SourceRepair::new(&api, source.map(str::to_owned), cluster.map(str::to_owned))
                    .is_none()
            );
        }
        let repair = reporter(&api);
        for kind in [
            ErrorKind::SourceConnectionFailed,
            ErrorKind::SourceAuthenticationError,
            ErrorKind::DestinationConnectionFailed,
            ErrorKind::SourceQueryFailed,
        ] {
            repair.startup(&EtlError::from((kind, "Test error"))).await;
        }
        assert!(timeout(Duration::from_millis(30), listener.accept()).await.is_err());
    }

    #[tokio::test]
    async fn startup_deadline_cancels_an_unconfirmed_save() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        let error =
            run_source_migrations(&unreachable_config().pipeline.pg_connection).await.unwrap_err();
        let ((), mut socket) = tokio::join!(repair.startup(&error), async {
            let socket = receive(&listener, "save").await;
            pause();
            advance(ATTEMPT_WINDOW).await;
            socket
        });
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn acknowledged_startup_waits_for_replacement_until_deadline() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        let error =
            run_source_migrations(&unreachable_config().pipeline.pg_connection).await.unwrap_err();
        let deadline = Instant::now() + ATTEMPT_WINDOW;
        let startup = repair.startup(&error);
        tokio::pin!(startup);

        tokio::select! {
            () = &mut startup => panic!("startup returned before replacement grace period"),

            () = async {
                acknowledge_save(&listener).await;
                let mut rollout = acknowledge_rollout(&listener).await;
                // Let the client consume the acknowledgement before advancing the clock.
                assert_eq!(rollout.read(&mut [0]).await.unwrap(), 0);
            } => {}
        }
        pause();
        assert!(timeout_at(deadline - Duration::from_secs(1), &mut startup).await.is_err());
        timeout_at(deadline + Duration::from_secs(1), startup).await.unwrap();
    }

    #[tokio::test]
    async fn store_startup_reports_only_the_configured_source_route() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        let mut config = unreachable_config();
        config.pipeline.store_pg_connection = Some(config.pipeline.pg_connection.clone());
        config.pipeline.pg_connection.host = "other.test".into();
        let result =
            crate::core::start_replicator_with_config(config.clone(), None, Some(&repair)).await;
        assert!(
            matches!(result, Err(crate::error::ReplicatorError::Etl(error)) if error.is_postgres_dial_failure())
        );
        assert!(timeout(Duration::from_millis(30), listener.accept()).await.is_err());

        config.pipeline.pg_connection.host = "127.0.0.1".into();
        let (result, ()) = tokio::join!(
            crate::core::start_replicator_with_config(config, None, Some(&repair)),
            async {
                acknowledge_save(&listener).await;
                // The backend declined rollout. Preserve the original startup
                // error.
                let mut socket = receive(&listener, "rollout").await;
                socket
                    .write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        );
        assert!(
            matches!(result, Err(crate::error::ReplicatorError::Etl(error)) if error.is_postgres_dial_failure())
        );
    }

    #[cfg(feature = "any-destination")]
    #[tokio::test]
    async fn worker_failures_coalesce_until_cooldown_and_shutdown_cancels_rollout() {
        let (listener, api) = server().await;
        let repair = reporter(&api);
        let (failures, receiver) = watch::channel(());
        failures.send(()).unwrap();
        let mut rollout = tokio::select! {
            () = repair.run(receiver) => panic!("reporter stopped before shutdown"),

            socket = async {
                let save = receive(&listener, "save").await;
                for _ in 0..10 {
                    failures.send(()).unwrap();
                }
                reply(save, r#"{"status":"source_saved"}"#).await;
                acknowledge_rollout(&listener).await;
                assert!(timeout(Duration::from_millis(100), listener.accept()).await.is_err());
                failures.send(()).unwrap();
                pause();
                advance(Duration::from_secs(91)).await;
                tokio::time::resume();
                assert!(timeout(Duration::from_millis(100), listener.accept()).await.is_err());

                failures.send(()).unwrap();
                acknowledge_save(&listener).await;
                receive(&listener, "rollout").await
            } => socket,
        };
        assert_eq!(
            timeout(Duration::from_secs(2), rollout.read(&mut [0])).await.unwrap().unwrap(),
            0
        );
        assert!(timeout(Duration::from_millis(30), listener.accept()).await.is_err());
    }
}
