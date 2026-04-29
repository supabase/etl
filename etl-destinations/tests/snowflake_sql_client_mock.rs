use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use etl_destinations::snowflake::{Config, Error, Result, SqlClient, TokenProvider};
use serde_json::json;
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{method, path},
};

const STATEMENTS_PATH: &str = "/api/v2/statements";
const POLL_HANDLE: &str = "test-handle-01";

/// Test token provider that returns a fixed bearer token and tracks how many
/// times the client called `invalidate_token`.
struct TestTokenProvider {
    invalidation_count: AtomicUsize,
}

impl TestTokenProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self { invalidation_count: AtomicUsize::new(0) })
    }

    fn invalidation_count(&self) -> usize {
        self.invalidation_count.load(Ordering::SeqCst)
    }
}

impl TokenProvider for TestTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok("test-token".into())
    }

    async fn invalidate_token(&self) {
        self.invalidation_count.fetch_add(1, Ordering::SeqCst);
    }
}

fn build_client(server: &MockServer) -> (SqlClient<TestTokenProvider>, Arc<TestTokenProvider>) {
    let auth = TestTokenProvider::new();
    let config =
        Config::new("test", "test-user", "TEST_DB", "PUBLIC").with_account_url(&server.uri());
    let client = SqlClient::new(config, Arc::clone(&auth), reqwest::Client::new());
    (client, auth)
}

/// Minimal 200 response body for DDL.
fn ddl_success() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({"message": "Statement executed successfully."}))
}

/// 202 async-in-progress response carrying a statement handle.
fn async_pending(handle: &str) -> ResponseTemplate {
    ResponseTemplate::new(202).set_body_json(json!({"statementHandle": handle}))
}

/// 422 SQL error response.
fn sql_error(handle: &str, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(422).set_body_json(json!({"statementHandle": handle, "message": message}))
}

fn poll_path_for(handle: &str) -> String {
    format!("{STATEMENTS_PATH}/{handle}")
}

/// Responds with each template in order, then repeats the last one forever.
struct Sequence(Mutex<Vec<ResponseTemplate>>);

impl Sequence {
    fn new(responses: Vec<ResponseTemplate>) -> Self {
        assert!(!responses.is_empty(), "Sequence must have at least one response");
        Self(Mutex::new(responses))
    }
}

impl Respond for Sequence {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let mut queue = self.0.lock().unwrap();
        if queue.len() > 1 { queue.remove(0) } else { queue[0].clone() }
    }
}

/// DDL succeeds when Snowflake returns 200.
#[tokio::test(start_paused = true)]
async fn execute_ddl_success() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(ddl_success())
        .expect(1)
        .mount(&server)
        .await;

    client.execute_ddl("CREATE TABLE t (id INT)").await.unwrap();
}

/// `table_exists` returns true when SHOW TABLES returns rows.
#[tokio::test(start_paused = true)]
async fn table_exists_returns_true_when_rows_present() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": [["2025-01-01", "MY_TABLE", "TEST_DB", "PUBLIC"]]})),
        )
        .expect(1)
        .mount(&server)
        .await;

    assert!(client.table_exists("MY_TABLE").await.unwrap());
}

/// `table_exists` returns false when SHOW TABLES returns no rows.
#[tokio::test(start_paused = true)]
async fn table_exists_returns_false_when_no_rows() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .expect(1)
        .mount(&server)
        .await;

    assert!(!client.table_exists("NO_SUCH_TABLE").await.unwrap());
}

/// 422 from Snowflake surfaces the SQL error message.
#[tokio::test(start_paused = true)]
async fn sql_error_422_returns_message() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(sql_error("err-01", "object 'FOO' does not exist"))
        .expect(1)
        .mount(&server)
        .await;

    let err = client.execute_ddl("DROP TABLE FOO").await.unwrap_err();
    match err {
        Error::Sql { message, .. } => assert!(message.contains("does not exist")),
        other => panic!("expected Sql error, got {other:?}"),
    }
}

/// Transient HTTP errors are retried with exponential backoff.
/// The first request fails with the given status; the second succeeds.
#[tokio::test(start_paused = true)]
async fn transient_errors_are_retried() {
    let cases: &[(u16, &str)] = &[
        (408, "Request Timeout"),
        (429, "Too Many Requests"),
        (500, "Internal Server Error"),
        (502, "Bad Gateway"),
        (503, "Service Unavailable"),
    ];

    for &(status, label) in cases {
        let server = MockServer::start().await;
        let (client, _auth) = build_client(&server);

        Mock::given(method("POST"))
            .and(path(STATEMENTS_PATH))
            .respond_with(Sequence::new(vec![
                ResponseTemplate::new(status).set_body_string(label),
                ddl_success(),
            ]))
            .expect(2)
            .mount(&server)
            .await;

        client
            .execute_ddl("DROP TABLE IF EXISTS test_table")
            .await
            .unwrap_or_else(|e| panic!("{status} ({label}) should be retried, got: {e:?}"));
    }
}

/// Non-retriable 4xx errors fail immediately without retrying.
#[tokio::test(start_paused = true)]
async fn non_retriable_errors_fail_immediately() {
    let cases: &[(u16, &str)] = &[
        (400, "Bad Request"),
        (403, "Forbidden"),
        (404, "Not Found"),
        (405, "Method Not Allowed"),
    ];

    for &(status, label) in cases {
        let server = MockServer::start().await;
        let (client, _auth) = build_client(&server);

        // Exactly one request — no retries.
        Mock::given(method("POST"))
            .and(path(STATEMENTS_PATH))
            .respond_with(ResponseTemplate::new(status).set_body_string(label))
            .expect(1)
            .mount(&server)
            .await;

        let err = client.execute_ddl("DROP TABLE IF EXISTS test_table").await.unwrap_err();
        match err {
            Error::HttpStatus { status: s, .. } => {
                assert_eq!(s, status, "case: {label}");
            }
            other => panic!("expected HttpStatus for {label}, got {other:?}"),
        }
    }
}

/// When all retry attempts are exhausted, the last error is returned.
/// 1 initial + 3 retries = 4 total requests.
#[tokio::test(start_paused = true)]
async fn retries_exhausted_returns_last_error() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    // Single-element Sequence repeats forever — every request gets 500.
    // 1 initial + 3 retries = 4 total requests.
    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(Sequence::new(vec![
            ResponseTemplate::new(500).set_body_string("always failing"),
        ]))
        .expect(4)
        .mount(&server)
        .await;

    let err = client.execute_ddl("DROP TABLE IF EXISTS test_table").await.unwrap_err();
    assert!(matches!(err, Error::HttpStatus { status: 500, .. }));
}

/// A single 401 triggers token invalidation, a fresh token fetch, and a
/// successful retry.
#[tokio::test(start_paused = true)]
async fn auth_failure_refreshes_token_and_retries() {
    let server = MockServer::start().await;
    let (client, auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(Sequence::new(vec![
            ResponseTemplate::new(401).set_body_string("Unauthorized"),
            ddl_success(),
        ]))
        .mount(&server)
        .await;

    client.execute_ddl("DROP TABLE IF EXISTS test_table").await.unwrap();
    assert_eq!(auth.invalidation_count(), 1, "token should be invalidated exactly once");
}

/// Two consecutive 401s result in failure — no infinite auth retry loop.
#[tokio::test(start_paused = true)]
async fn repeated_auth_failure_stops() {
    let server = MockServer::start().await;
    let (client, auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
        .mount(&server)
        .await;

    let err = client.execute_ddl("DROP TABLE IF EXISTS test_table").await.unwrap_err();
    assert!(matches!(err, Error::HttpStatus { status: 401, .. }));
    assert_eq!(auth.invalidation_count(), 1, "invalidated once before giving up");
}

/// 202 triggers async polling; the first poll returns 200 (success).
#[tokio::test(start_paused = true)]
async fn polling_completes_on_first_check() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    // Submit → 202 with statement handle.
    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(async_pending(POLL_HANDLE))
        .expect(1)
        .mount(&server)
        .await;

    // First poll → 200 success.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(ddl_success())
        .expect(1)
        .mount(&server)
        .await;

    client.execute_ddl("CREATE TABLE t (id INT)").await.unwrap();
}

/// Multiple 202 poll responses before the final 200.
#[tokio::test]
async fn polling_succeeds_after_multiple_rounds() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(async_pending(POLL_HANDLE))
        .expect(1)
        .mount(&server)
        .await;

    // First three polls return 202.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(async_pending(POLL_HANDLE))
        .up_to_n_times(3)
        .expect(3)
        .mount(&server)
        .await;

    // After the 202s are exhausted, the next poll returns 200.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(ddl_success())
        .expect(1)
        .mount(&server)
        .await;

    client.execute_ddl("CREATE TABLE t (id INT)").await.unwrap();
}

/// Poll returns 422 — the SQL error from the async statement is surfaced.
#[tokio::test(start_paused = true)]
async fn polling_fails_with_sql_error() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(async_pending(POLL_HANDLE))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(sql_error(POLL_HANDLE, "async statement failed"))
        .expect(1)
        .mount(&server)
        .await;

    let err = client.execute_ddl("CREATE TABLE bad (id INT)").await.unwrap_err();
    match err {
        Error::Sql { message, .. } => assert!(message.contains("async statement failed")),
        other => panic!("expected Sql error, got {other:?}"),
    }
}

/// 422 after several 202 polls — the error surfaces after intermediate polls.
#[tokio::test]
async fn polling_fails_after_intermediate_polls() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(async_pending(POLL_HANDLE))
        .expect(1)
        .mount(&server)
        .await;

    // First two polls return 202.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(async_pending(POLL_HANDLE))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;

    // After the 202s are exhausted, the next poll returns 422.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(sql_error(POLL_HANDLE, "compilation error"))
        .expect(1)
        .mount(&server)
        .await;

    let err = client.execute_ddl("CREATE TABLE bad (id INT)").await.unwrap_err();
    match err {
        Error::Sql { message, .. } => assert!(message.contains("compilation error")),
        other => panic!("expected Sql error, got {other:?}"),
    }
}

/// When the poll never resolves (always 202), the client times out.
#[tokio::test(start_paused = true)]
async fn polling_times_out() {
    let server = MockServer::start().await;
    let (client, _auth) = build_client(&server);

    Mock::given(method("POST"))
        .and(path(STATEMENTS_PATH))
        .respond_with(async_pending(POLL_HANDLE))
        .expect(1)
        .mount(&server)
        .await;

    // Always returns 202 — the statement never completes.
    Mock::given(method("GET"))
        .and(path(poll_path_for(POLL_HANDLE)))
        .respond_with(async_pending(POLL_HANDLE))
        .mount(&server)
        .await;

    let err = client.execute_ddl("CREATE TABLE forever (id INT)").await.unwrap_err();
    match err {
        Error::Sql { message, .. } => {
            assert!(message.contains("did not complete within"), "got: {message}");
        }
        other => panic!("expected timeout Sql error, got {other:?}"),
    }
}
