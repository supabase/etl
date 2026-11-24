//! Best-effort reporting of a managed source dial failure. Kubernetes owns
//! replacement.

use std::{env, time::Duration};

use configcat::Client;
use etl::error::EtlError;
use etl_config::shared::validate_supabase_project_ref;
use reqwest::StatusCode;
use serde::Deserialize;
#[cfg(feature = "any-destination")]
use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until, timeout_at};
use tracing::{info, warn};

use crate::management_api::ManagementApiClient;

const ENABLED_FLAG: &str = "etl_automatic_source_repair";
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
    flags: &'a Client,
    source_id: String,
    cluster_id: String,
}

impl<'a> SourceRepair<'a> {
    /// Only managed processes with complete routing, credentials and cohort
    /// evaluation can report.
    pub(crate) fn from_env(
        api: Option<&'a ManagementApiClient>,
        flags: Option<&'a Client>,
    ) -> Option<Self> {
        Self::new(api?, flags?, env::var(SOURCE_ID_ENV).ok()?, env::var(CLUSTER_ID_ENV).ok()?)
    }

    fn new(
        api: &'a ManagementApiClient,
        flags: &'a Client,
        source_id: String,
        cluster_id: String,
    ) -> Option<Self> {
        validate_supabase_project_ref(api.project_ref()).ok()?;
        if source_id.is_empty()
            || !source_id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            || cluster_id.trim().is_empty()
        {
            return None;
        }
        Some(Self { api, flags, source_id, cluster_id })
    }

    /// Gives an eligible startup error one bounded opportunity for repair and
    /// replacement before returning the original error to the supervisor.
    pub(crate) async fn startup(&self, error: &EtlError) {
        if error.is_source_network_failure() {
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
                // A cohort can be disabled between save and rollout.
                if !self.flags.get_value(ENABLED_FLAG, false, None).await {
                    return false;
                }
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
    use configcat::{MapDataSource, OverrideBehavior, Value};
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

    fn flags(enabled: bool) -> Client {
        Client::builder("local-test")
            .overrides(
                Box::new(MapDataSource::from([(ENABLED_FLAG, Value::Bool(enabled))])),
                OverrideBehavior::LocalOnly,
            )
            .build()
            .unwrap()
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

    fn reporter<'a>(api: &'a ManagementApiClient, flags: &'a Client) -> SourceRepair<'a> {
        SourceRepair::new(api, flags, "42".into(), "cluster-a".into()).unwrap()
    }

    /// Reading the full request before dropping the socket models a lost
    /// acknowledgement.
    async fn receive(listener: &TcpListener, phase: Phase) -> TcpStream {
        let (socket, _) =
            timeout(Duration::from_secs(8), listener.accept()).await.unwrap().unwrap();
        let mut stream = BufReader::new(socket);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        assert_eq!(
            line,
            format!(
                "POST /system/replication/{PROJECT}/sources/42?cluster_id=cluster-a&\
                 repair_phase={} HTTP/1.1\r\n",
                phase.query()
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

    async fn reply(mut socket: TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }

    #[tokio::test]
    async fn retries_each_lost_reply_before_advancing_to_the_next_phase() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        let (confirmed, ()) = tokio::join!(repair.report(), async {
            drop(receive(&listener, Phase::Save).await);
            reply(receive(&listener, Phase::Save).await, r#"{"status":"source_saved"}"#).await;
            drop(receive(&listener, Phase::Rollout).await);
            reply(receive(&listener, Phase::Rollout).await, r#"{"status":"rollout_requested"}"#)
                .await;
        });
        assert!(confirmed);
    }

    #[tokio::test]
    async fn rejects_wrong_or_oversized_save_acknowledgements() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        for body in [r#"{"status":"rollout_requested"}"#.to_owned(), " ".repeat(MAX_ACK_BYTES + 1)]
        {
            let (confirmed, ()) = tokio::join!(repair.report(), async {
                reply(receive(&listener, Phase::Save).await, &body).await;
            });
            assert!(!confirmed);
        }
    }

    #[tokio::test]
    async fn replacement_resumes_after_the_first_caller_disappears() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let first = reporter(&api, &flags);
        tokio::select! {
            _ = first.report() => panic!("first caller completed before save acknowledgement"),

            // The backend committed this request, then the pod was replaced.
            socket = receive(&listener, Phase::Save) => drop(socket),
        }
        let replacement = reporter(&api, &flags);
        let (confirmed, ()) = tokio::join!(replacement.report(), async {
            reply(receive(&listener, Phase::Save).await, r#"{"status":"source_saved"}"#).await;
            reply(receive(&listener, Phase::Rollout).await, r#"{"status":"rollout_requested"}"#)
                .await;
        });
        assert!(confirmed);
    }

    #[tokio::test]
    async fn incomplete_routing_disabled_cohort_and_unrelated_errors_do_not_report() {
        let (listener, api) = server().await;
        let off = flags(false);
        for (source, cluster) in [("", "a"), ("1", " "), ("../emails", "a")] {
            assert!(SourceRepair::new(&api, &off, source.into(), cluster.into()).is_none());
        }
        assert!(SourceRepair::from_env(None, Some(&off)).is_none());
        assert!(SourceRepair::from_env(Some(&api), None).is_none());
        assert!(!reporter(&api, &off).report().await);
        let on = flags(true);
        let repair = reporter(&api, &on);
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
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        let error =
            run_source_migrations(&unreachable_config().pipeline.pg_connection).await.unwrap_err();
        let ((), mut socket) = tokio::join!(repair.startup(&error), async {
            let socket = receive(&listener, Phase::Save).await;
            pause();
            advance(ATTEMPT_WINDOW).await;
            socket
        });
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn store_startup_reports_only_the_configured_source_route() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        let mut config = unreachable_config();
        config.pipeline.store_pg_connection = Some(config.pipeline.pg_connection.clone());
        config.pipeline.pg_connection.host = "other.test".into();
        let result =
            crate::core::start_replicator_with_config(config.clone(), None, Some(&repair)).await;
        assert!(
            matches!(result, Err(crate::error::ReplicatorError::Etl(error)) if error.is_source_network_failure())
        );
        assert!(timeout(Duration::from_millis(30), listener.accept()).await.is_err());

        config.pipeline.pg_connection.host = "127.0.0.1".into();
        let (result, ()) = tokio::join!(
            crate::core::start_replicator_with_config(config, None, Some(&repair)),
            async {
                reply(receive(&listener, Phase::Save).await, r#"{"status":"source_saved"}"#).await;
                // The backend declined rollout. Preserve the original startup
                // error.
                let mut socket = receive(&listener, Phase::Rollout).await;
                socket
                    .write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        );
        assert!(
            matches!(result, Err(crate::error::ReplicatorError::Etl(error)) if error.is_source_network_failure())
        );
    }

    #[cfg(feature = "any-destination")]
    #[tokio::test]
    async fn cooldown_requires_a_fresh_failure_and_allows_a_later_report() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        let (failures, receiver) = watch::channel(());
        failures.send(()).unwrap();
        tokio::select! {
            () = repair.run(receiver) => panic!("reporter stopped before shutdown"),

            () = async {
                reply(receive(&listener, Phase::Save).await, r#"{"status":"source_saved"}"#).await;
                let rollout = receive(&listener, Phase::Rollout).await;
                failures.send(()).unwrap();
                reply(rollout, r#"{"status":"rollout_requested"}"#).await;
                assert!(timeout(Duration::from_millis(100), listener.accept()).await.is_err());
                failures.send(()).unwrap();
                pause();
                advance(Duration::from_secs(91)).await;
                tokio::time::resume();
                assert!(timeout(Duration::from_millis(100), listener.accept()).await.is_err());
                failures.send(()).unwrap();
                drop(receive(&listener, Phase::Save).await);
            } => {}
        }
    }

    #[cfg(feature = "any-destination")]
    #[tokio::test]
    async fn concurrent_worker_failures_coalesce_and_shutdown_cancels_rollout() {
        let (listener, api) = server().await;
        let flags = flags(true);
        let repair = reporter(&api, &flags);
        let (failures, receiver) = watch::channel(());
        failures.send(()).unwrap();
        let mut rollout = {
            let run = repair.run(receiver);
            tokio::pin!(run);
            tokio::select! {
                () = &mut run => panic!("reporter stopped before shutdown"),

                socket = async {
                    let save = receive(&listener, Phase::Save).await;
                    for _ in 0..10 { failures.send(()).unwrap(); }
                    reply(save, r#"{"status":"source_saved"}"#).await;
                    receive(&listener, Phase::Rollout).await
                } => socket,
            }
        };
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(2), rollout.read(&mut byte)).await.unwrap().unwrap(),
            0
        );
        assert!(timeout(Duration::from_millis(30), listener.accept()).await.is_err());
    }
}
