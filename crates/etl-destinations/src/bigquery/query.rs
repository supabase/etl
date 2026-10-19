//! Query submission, completion, and retries for BigQuery DDL and metadata.
//!
//! Request retries preserve identity; only confirmed failed jobs get a new
//! request ID. Submission, polling, pagination, and job retries share one
//! deadline. The SQL callers must never interpret HTTP acceptance as
//! completion.

use std::{error::Error as _, future::Future};

use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use gcp_bigquery_client::{
    Client,
    error::BQError,
    model::{
        get_query_results_parameters::GetQueryResultsParameters, job_reference::JobReference,
        query_request::QueryRequest, query_response::QueryResponse,
    },
};
use rand::random_range;
use tokio::time::{Duration, Instant, sleep, timeout_at};
use tracing::warn;
use uuid::Uuid;

use crate::{
    bigquery::client::bq_error_to_etl_error,
    retry::{RetryPolicy, retry_with_backoff},
};

/// Total budget for query submission, transient retries, and completion.
///
/// ETL uses queries for DDL and metadata. Ten minutes tolerates temporary
/// service delays while keeping submission retries inside BigQuery's
/// 15-minute request-ID deduplication window. This is an operational budget,
/// not a Google service guarantee.
/// See <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query>.
const QUERY_TIMEOUT: Duration = Duration::from_secs(600);

/// Requested server-side wait, matching BigQuery's default.
///
/// HTTP latency can exceed this hint. [`QUERY_TIMEOUT`] bounds local waiting
/// without cancelling the remote job.
const QUERY_WAIT_TIMEOUT_MS: i32 = 10_000;

/// Avoids rapid polling when BigQuery returns early with an incomplete job.
const QUERY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum backoff, using one of BigQuery's typical values of 32 or 64 seconds.
/// See <https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_api_requests>.
const QUERY_MAX_RETRY_DELAY: Duration = Duration::from_secs(32);

/// Request retries absorb brief outages without starting another query job.
///
/// Twenty retries allow about eight and a half minutes of backoff for an
/// immediately failing request. The shared ten-minute deadline also includes
/// network time, polling, and any job retries. The attempt count is an ETL
/// bound, not a BigQuery requirement.
const QUERY_REQUEST_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 20,
    initial_delay: Duration::from_secs(1),
    max_delay: QUERY_MAX_RETRY_DELAY,
};

/// Confirmed failed jobs get at most four replacements to bound repeated work.
///
/// This is independent of the per-request retry limit and cannot renew the
/// overall deadline. Google documents which failures permit a new job, but
/// does not prescribe an attempt count.
const QUERY_JOB_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 4,
    initial_delay: Duration::from_secs(1),
    max_delay: QUERY_MAX_RETRY_DELAY,
};

/// BigQuery response reasons documented as identifying a created, failed query
/// job that should be retried as a new job.
/// See <https://cloud.google.com/bigquery/docs/error-messages#jobBackendError>.
const FAILED_BIGQUERY_QUERY_JOB_REASONS: &[&str] =
    &["jobBackendError", "jobInternalError", "jobRateLimitExceeded"];

/// Adds up to one second of jitter without shortening Google's base delay.
fn query_retry_delay(delay: Duration) -> Duration {
    let jitter = Duration::from_millis(random_range(0..=1000));
    delay.saturating_add(jitter).min(QUERY_MAX_RETRY_DELAY)
}

/// Returns whether retrying a query error requires creating a new query job.
fn query_retry_requires_new_request_id(error: &BQError) -> bool {
    let BQError::ResponseError { error } = error else {
        return false;
    };

    error.error.errors.iter().any(|nested_error| {
        nested_error
            .get("reason")
            .is_some_and(|reason| FAILED_BIGQUERY_QUERY_JOB_REASONS.contains(&reason.as_str()))
    })
}

/// Generates an idempotency key accepted by BigQuery request-ID fields.
fn generate_bigquery_request_id() -> String {
    Uuid::new_v4().simple().to_string()
}

/// Logs a transient BigQuery query retry.
fn log_query_retry(attempt: crate::retry::RetryAttempt<'_, BQError>) {
    warn!(
        retry_index = attempt.retry_index,
        max_retries = attempt.max_retries,
        sleep_ms = attempt.sleep_delay.as_millis(),
        error = %attempt.error,
        "retrying transient bigquery query error"
    );
}

/// Reports an ambiguous query outcome without authorizing automatic replay.
///
/// A local deadline does not cancel remote DDL. `DestinationTimeout` would
/// trigger a worker retry that could race a still-running view replacement.
fn query_timeout_error(request_id: &str, reference: Option<&JobReference>) -> EtlError {
    let job_id = reference.and_then(|reference| reference.job_id.as_deref()).unwrap_or("unknown");
    let location =
        reference.and_then(|reference| reference.location.as_deref()).unwrap_or("unknown");
    etl_error!(
        ErrorKind::DestinationQueryFailed,
        "BigQuery query completion timed out",
        format!(
            "Completion was not confirmed within {} seconds; request_id={request_id}, \
             job_id={job_id}, location={location}. The remote job may still be running. Verify \
             its outcome before manually retrying.",
            QUERY_TIMEOUT.as_secs()
        )
    )
}

/// Completes one submitted job and collects its result pages within the
/// deadline.
///
/// The original job identity and location survive responses that omit them.
/// Successful pending responses and transport retries do not renew the budget.
async fn complete_query<F, Fut>(
    mut response: QueryResponse,
    deadline: Instant,
    request_id: &str,
    mut poll: F,
) -> EtlResult<QueryResponse>
where
    F: FnMut(JobReference, Option<String>) -> Fut,
    Fut: Future<Output = EtlResult<QueryResponse>>,
{
    if response.job_complete == Some(true)
        && response.page_token.as_ref().is_none_or(String::is_empty)
    {
        return Ok(response);
    }

    let reference = response
        .job_reference
        .take()
        .filter(|reference| reference.job_id.as_ref().is_some_and(|id| !id.is_empty()))
        .ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "BigQuery query response has no job identity",
                "Cannot complete the query or retrieve the remaining result pages"
            )
        })?;

    let mut response = timeout_at(deadline, async {
        while response.job_complete != Some(true) {
            response = poll(reference.clone(), None).await?;
            if response.job_complete != Some(true) {
                sleep(QUERY_POLL_INTERVAL).await;
            }
        }

        // INFORMATION_SCHEMA listings must include every obsolete version,
        // even when BigQuery splits the result across multiple pages.
        while let Some(page_token) = response.page_token.take().filter(|token| !token.is_empty()) {
            let page = poll(reference.clone(), Some(page_token)).await?;
            if page.job_complete != Some(true) {
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "BigQuery returned an incomplete result page for a completed job"
                ));
            }

            response.rows.get_or_insert_with(Vec::new).extend(page.rows.unwrap_or_default());
            response.page_token = page.page_token;
        }

        Ok(response)
    })
    .await
    .map_err(|_| query_timeout_error(request_id, Some(&reference)))??;

    response.job_reference = Some(reference);
    Ok(response)
}

/// Submits and completes one job, retrying requests with stable identities.
async fn execute_query_attempt(
    client: &Client,
    project_id: &str,
    request: QueryRequest,
    deadline: Instant,
) -> EtlResult<QueryResponse> {
    let request_id = request.request_id.as_deref().unwrap_or("unknown");
    // Tokio can poll an immediately-ready future before observing expiry.
    // Never start another remote job after the shared budget has elapsed.
    if Instant::now() >= deadline {
        return Err(query_timeout_error(request_id, None));
    }

    let response = timeout_at(
        deadline,
        retry_with_backoff(
            QUERY_REQUEST_RETRY_POLICY,
            query_retry_requires_new_request_id,
            query_retry_delay,
            log_query_retry,
            || client.job().query(project_id, request.clone()),
        ),
    )
    .await
    .map_err(|_| query_timeout_error(request_id, None))?
    .map_err(|failure| bq_error_to_etl_error(failure.last_error))?;

    // Dry runs validate without creating an executable job to poll.
    if request.dry_run == Some(true) {
        return Ok(response);
    }

    let max_results = request.max_results;
    complete_query(response, deadline, request_id, |reference, page_token| async move {
        let job_id = reference.job_id.as_deref().ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "Incomplete BigQuery query has no job identity"
            )
        })?;

        retry_with_backoff(
            QUERY_REQUEST_RETRY_POLICY,
            query_retry_requires_new_request_id,
            query_retry_delay,
            log_query_retry,
            || {
                client.job().get_query_results(
                    project_id,
                    job_id,
                    GetQueryResultsParameters {
                        location: reference.location.clone(),
                        max_results,
                        page_token: page_token.clone(),
                        timeout_ms: Some(QUERY_WAIT_TIMEOUT_MS),
                        ..Default::default()
                    },
                )
            },
        )
        .await
        .map(QueryResponse::from)
        .map_err(|failure| bq_error_to_etl_error(failure.last_error))
    })
    .await
}

/// Retries confirmed failed jobs while retaining one overall deadline.
///
/// Only documented terminal job failures permit a fresh request ID, whether
/// reported by submission or polling. Request-level retries stay inside `run`.
async fn retry_query_jobs<F, Fut>(mut request: QueryRequest, mut run: F) -> EtlResult<QueryResponse>
where
    F: FnMut(QueryRequest, Instant) -> Fut,
    Fut: Future<Output = EtlResult<QueryResponse>>,
{
    let deadline = Instant::now() + QUERY_TIMEOUT;
    request.timeout_ms.get_or_insert(QUERY_WAIT_TIMEOUT_MS);
    let mut initial_request_id = request.request_id.take();

    retry_with_backoff(
        QUERY_JOB_RETRY_POLICY,
        |error: &EtlError| {
            !error
                .source()
                .and_then(|source| source.downcast_ref::<BQError>())
                .is_some_and(query_retry_requires_new_request_id)
        },
        |delay| query_retry_delay(delay).min(deadline.saturating_duration_since(Instant::now())),
        |attempt| {
            warn!(
                retry_index = attempt.retry_index,
                max_retries = attempt.max_retries,
                sleep_ms = attempt.sleep_delay.as_millis(),
                error = %attempt.error,
                "retrying failed bigquery job with a new request id"
            );
        },
        || {
            // Only confirmed job failures reach another attempt. Preserve the
            // caller's first ID, then allocate a fresh ID for each replacement.
            request.request_id =
                Some(initial_request_id.take().unwrap_or_else(generate_bigquery_request_id));

            run(request.clone(), deadline)
        },
    )
    .await
    .map_err(|failure| failure.last_error)
}

/// Submits a query, waits for completion, and collects every result page.
///
/// Production and test queries share request retries, job retries, and a single
/// deadline. A pending SELECT must never be mistaken for an empty result set.
pub(super) async fn execute_query(
    client: &Client,
    project_id: &str,
    request: QueryRequest,
) -> EtlResult<QueryResponse> {
    retry_query_jobs(request, |request, deadline| {
        execute_query_attempt(client, project_id, request, deadline)
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use etl::{
        error::{ErrorKind, Retryability},
        etl_error,
    };
    use gcp_bigquery_client::{
        Client,
        auth::Authenticator,
        client_builder::ClientBuilder,
        error::{BQError, NestedResponseError, ResponseError},
        model::{
            job_reference::JobReference,
            query_request::QueryRequest,
            query_response::{QueryResponse, ResultSet},
        },
    };
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
        time::{Duration, Instant, timeout},
    };
    use tokio_util::task::AbortOnDropHandle;

    use crate::{
        bigquery::{
            client::bq_error_to_etl_error,
            query::{
                QUERY_MAX_RETRY_DELAY, QUERY_TIMEOUT, QUERY_WAIT_TIMEOUT_MS, complete_query,
                execute_query, execute_query_attempt, query_retry_delay,
                query_retry_requires_new_request_id, retry_query_jobs,
            },
        },
        retry::ClassifiedError,
    };

    /// Counts request authentication and supplies an inert fixture token.
    #[derive(Clone, Default)]
    struct TestAuthenticator(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl Authenticator for TestAuthenticator {
        async fn access_token(&self) -> Result<String, BQError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok("placeholder-token".to_owned())
        }
    }

    /// Serves a finite response script and captures requests through the real
    /// BigQuery SDK, including JSON bodies and result-page parameters.
    async fn query_http_fixture(
        responses: Vec<(u16, Value)>,
    ) -> (Client, AbortOnDropHandle<Vec<(String, Value)>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());

        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut requests = Vec::new();

            for (status, body) in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut lines = BufReader::new(stream).lines();
                let request_line = lines.next_line().await.unwrap().unwrap();
                let mut content_length = 0;

                loop {
                    let header = lines.next_line().await.unwrap().unwrap();
                    if header.is_empty() {
                        break;
                    }

                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap();
                    }
                }

                let mut reader = lines.into_inner();
                let mut bytes = vec![0; content_length];
                reader.read_exact(&mut bytes).await.unwrap();

                let request_body = if bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes).unwrap()
                };
                requests.push((request_line, request_body));

                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\ncontent-type: \
                     application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                reader.get_mut().write_all(response.as_bytes()).await.unwrap();
            }

            requests
        }));

        let client = ClientBuilder::new()
            .with_v2_base_url(base_url)
            .build_from_authenticator(Arc::new(TestAuthenticator::default()))
            .await
            .unwrap();

        (client, server)
    }

    /// Verifies the composed executor preserves request identity across
    /// transient errors, replaces failed jobs, and retries the same result
    /// page.
    #[tokio::test]
    async fn query_http_retries_preserve_job_and_page_boundaries() {
        let error = |code, reason| {
            json!({
                "error": {"code": code, "message": "Temporary query failure", "errors": [{"reason": reason}]}
            })
        };
        let pending = |job_id| {
            json!({
                "jobComplete": false,
                "jobReference": {"jobId": job_id, "location": "europe-west1"}
            })
        };
        let (client, server) = query_http_fixture(vec![
            (503, error(503, "backendError")),
            (200, pending("first_job")),
            (400, error(400, "jobRateLimitExceeded")),
            (200, pending("replacement_job")),
            (503, error(503, "backendError")),
            (
                200,
                json!({
                    "jobComplete": true,
                    "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                    "rows": [{"f": [{"v": "1"}]}], "pageToken": "next-page"
                }),
            ),
            (429, error(429, "rateLimitExceeded")),
            (200, json!({"jobComplete": true, "rows": [{"f": [{"v": "2"}]}], "pageToken": ""})),
        ])
        .await;
        let mut request = QueryRequest::new("select 1 as value");
        request.max_results = Some(1);
        request.request_id = Some("caller-request-id".to_owned());

        let response =
            timeout(Duration::from_secs(30), execute_query(&client, "test-project", request))
                .await
                .unwrap()
                .unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 8);
        assert_eq!(requests[0].1, requests[1].1);
        assert_eq!(requests[1].1["requestId"], "caller-request-id");
        assert_ne!(requests[1].1["requestId"], requests[3].1["requestId"]);
        assert!(requests[2].0.contains("/queries/first_job?"));
        assert!(requests[4].0.contains("/queries/replacement_job?"));
        assert_eq!(requests[4], requests[5]);
        assert_eq!(requests[6], requests[7]);
        assert!(requests[6].0.contains("pageToken=next-page"));
        for index in [2, 4, 5, 6, 7] {
            assert!(requests[index].0.contains("location=europe-west1"));
            assert!(requests[index].0.contains("maxResults=1"));
            assert!(requests[index].0.contains("timeoutMs=10000"));
        }

        let mut rows = ResultSet::new_from_query_response(response);
        for expected in [1, 2] {
            assert!(rows.next_row());
            assert_eq!(rows.get_i64(0).unwrap(), Some(expected));
        }
        assert!(!rows.next_row());
    }

    /// Invalid requests and rejected queries stop without replay; dry runs
    /// succeed without creating a job to poll.
    #[tokio::test]
    async fn query_errors_and_dry_runs_do_not_start_replacement_jobs() {
        let auth = TestAuthenticator::default();
        let client = ClientBuilder::new()
            .with_v2_base_url("invalid-url".to_owned())
            .build_from_authenticator(Arc::new(auth.clone()))
            .await
            .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            execute_query(&client, "test-project", QueryRequest::new("select 1")),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(auth.0.load(Ordering::Relaxed), 1);

        for dry_run in [false, true] {
            let response = if dry_run {
                (200, json!({"totalBytesProcessed": "0"}))
            } else {
                (
                    403,
                    json!({"error": {
                        "code": 403, "message": "Access denied",
                        "errors": [{"reason": "accessDenied"}]
                    }}),
                )
            };
            let (client, server) = query_http_fixture(vec![response]).await;
            let mut request = QueryRequest::new("select 1");
            request.dry_run = Some(dry_run);

            let result =
                timeout(Duration::from_secs(5), execute_query(&client, "test-project", request))
                    .await
                    .unwrap();
            assert_eq!(result.is_ok(), dry_run);

            let requests = server.await.unwrap();
            assert_eq!(requests.len(), 1);
            let request_id = requests[0].1["requestId"].as_str().unwrap();
            assert_eq!(request_id.len(), 32);
            assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    /// Expiry must be checked before polling an operation that could send DDL.
    #[tokio::test]
    async fn expired_query_budget_does_not_submit_a_job() {
        let auth = TestAuthenticator::default();
        let client = ClientBuilder::new()
            .with_v2_base_url("http://127.0.0.1:1".to_owned())
            .build_from_authenticator(Arc::new(auth.clone()))
            .await
            .unwrap();
        let error = execute_query_attempt(
            &client,
            "test-project",
            QueryRequest::new("select 1"),
            Instant::now(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(auth.0.load(Ordering::Relaxed), 0);
    }

    /// Accepted DDL must not become durable until the same job completes.
    #[tokio::test(start_paused = true)]
    async fn incomplete_query_waits_for_the_original_job() {
        let mut polls = 0;
        let response = QueryResponse {
            job_complete: Some(false),
            job_reference: Some(JobReference {
                job_id: Some("pending_job".to_owned()),
                location: Some("europe-west1".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let completed =
            complete_query(
                response,
                Instant::now() + QUERY_TIMEOUT,
                "test-request",
                |reference, page_token| {
                    assert!(page_token.is_none());
                    assert_eq!(reference.job_id.as_deref(), Some("pending_job"));
                    assert_eq!(reference.location.as_deref(), Some("europe-west1"));
                    polls += 1;
                    let complete = polls == 2;
                    async move {
                        Ok(QueryResponse { job_complete: Some(complete), ..Default::default() })
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(polls, 2);
        assert_eq!(completed.job_complete, Some(true));
        let reference = completed.job_reference.unwrap();
        assert_eq!(reference.job_id.as_deref(), Some("pending_job"));
        assert_eq!(reference.location.as_deref(), Some("europe-west1"));
    }

    /// Cleanup listings must retain every page and the original job location.
    #[tokio::test]
    async fn complete_query_collects_all_pages() {
        let response: QueryResponse = serde_json::from_value(serde_json::json!({
            "jobComplete": true,
            "jobReference": {"jobId": "completed_job", "location": "europe-west1"},
            "pageToken": "second-page",
            "schema": {"fields": [{"name": "table_name", "type": "STRING"}]},
            "rows": [{"f": [{"v": "first"}]}]
        }))
        .unwrap();
        let mut polls = 0;

        let completed = complete_query(
            response,
            Instant::now() + QUERY_TIMEOUT,
            "test-request",
            |reference, page_token| {
                assert_eq!(reference.job_id.as_deref(), Some("completed_job"));
                assert_eq!(reference.location.as_deref(), Some("europe-west1"));
                polls += 1;
                assert_eq!(
                    page_token.as_deref(),
                    Some(if polls == 1 { "second-page" } else { "third-page" })
                );

                let response = serde_json::from_value(serde_json::json!({
                    "jobComplete": true,
                    "pageToken": if polls == 1 { Some("third-page") } else { None },
                    "rows": [{"f": [{"v": if polls == 1 { "second" } else { "third" }}]}]
                }))
                .unwrap();
                async move { Ok(response) }
            },
        )
        .await
        .unwrap();

        assert_eq!(polls, 2);
        assert!(completed.page_token.is_none());
        let mut rows = ResultSet::new_from_query_response(completed);
        for expected in ["first", "second", "third"] {
            assert!(rows.next_row());
            assert_eq!(rows.get_string(0).unwrap().as_deref(), Some(expected));
        }
        assert!(!rows.next_row());
    }

    /// Malformed pending responses cannot be mistaken for successful empty DDL.
    #[tokio::test]
    async fn incomplete_query_without_job_identity_fails() {
        for reference in [
            None,
            Some(JobReference::default()),
            Some(JobReference { job_id: Some(String::new()), ..Default::default() }),
        ] {
            let response = QueryResponse {
                job_complete: Some(false),
                job_reference: reference,
                ..Default::default()
            };
            let error = complete_query(
                response,
                Instant::now() + QUERY_TIMEOUT,
                "test-request",
                |_, _| async { panic!("invalid job identity must be rejected before polling") },
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        }
    }

    /// Polling failure must reach the caller rather than publishing readiness.
    #[tokio::test]
    async fn incomplete_query_propagates_polling_failure() {
        let response = QueryResponse {
            job_complete: Some(false),
            job_reference: Some(JobReference {
                job_id: Some("pending_job".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let error = complete_query(
            response,
            Instant::now() + QUERY_TIMEOUT,
            "test-request",
            |_, _| async {
                Err(etl_error!(ErrorKind::DestinationQueryFailed, "Injected query failure"))
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
    }

    /// Pending responses and stalled requests share one non-renewable budget.
    #[tokio::test(start_paused = true)]
    async fn incomplete_query_deadline_preserves_unknown_outcome() {
        for stalled in [false, true] {
            let start = Instant::now();
            let deadline = start + Duration::from_secs(5);
            let response = QueryResponse {
                job_complete: Some(false),
                job_reference: Some(JobReference {
                    job_id: Some("pending_job".to_owned()),
                    location: Some("europe-west1".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let error = complete_query(response, deadline, "test-request", |_, _| async move {
                if stalled {
                    std::future::pending::<()>().await;
                }
                Ok(QueryResponse { job_complete: Some(false), ..Default::default() })
            })
            .await
            .unwrap_err();
            assert_eq!(Instant::now() - start, Duration::from_secs(5));
            // DestinationTimeout would authorize unsafe automatic DDL replay.
            assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
            let detail = error.detail().unwrap();
            assert!(detail.contains("request_id=test-request"));
            assert!(detail.contains("job_id=pending_job"));
            assert!(detail.contains("location=europe-west1"));
        }
    }

    /// Builds the structured error returned by a failed BigQuery request.
    fn query_response_error(code: i64, reason: &str) -> BQError {
        BQError::ResponseError {
            error: ResponseError {
                error: NestedResponseError {
                    code,
                    errors: vec![[("reason".to_owned(), reason.to_owned())].into_iter().collect()],
                    message: "Placeholder BigQuery error".to_owned(),
                    status: "PLACEHOLDER_STATUS".to_owned(),
                },
            },
        }
    }

    #[test]
    fn query_retry_classification_matches_bigquery_job_lifecycle() {
        let cases = [
            ("jobBackendError", 400, Retryability::Retryable, true),
            ("jobInternalError", 400, Retryability::Retryable, true),
            ("jobRateLimitExceeded", 400, Retryability::Retryable, true),
            ("backendError", 500, Retryability::Retryable, false),
            ("internalError", 500, Retryability::Retryable, false),
            ("rateLimitExceeded", 403, Retryability::Retryable, false),
            ("unknownServerError", 503, Retryability::Retryable, false),
            ("tooManyRequests", 429, Retryability::Retryable, false),
            ("invalid", 400, Retryability::Permanent, false),
            ("invalidQuery", 400, Retryability::Permanent, false),
            ("accessDenied", 403, Retryability::Permanent, false),
            ("quotaExceeded", 403, Retryability::Permanent, false),
            ("notFound", 404, Retryability::Permanent, false),
            ("resourcesExceeded", 400, Retryability::Permanent, false),
            ("timeout", 400, Retryability::Permanent, false),
        ];

        for (reason, code, expected_retryability, expected_new_request_id) in cases {
            let error = query_response_error(code, reason);

            assert_eq!(error.retryability(), expected_retryability, "{reason}");
            assert_eq!(
                query_retry_requires_new_request_id(&error),
                expected_new_request_id,
                "{reason}"
            );
        }
    }

    /// Converting provider errors must preserve their types without making
    /// permanent data, configuration, or authentication failures retryable.
    #[tokio::test(start_paused = true)]
    async fn typed_query_errors_preserve_classification_without_replay() {
        for (provider_error, kind) in [
            (
                BQError::InvalidColumnType {
                    col_index: 0,
                    col_type: "STRING".to_owned(),
                    type_requested: "INT64".to_owned(),
                },
                ErrorKind::ConversionError,
            ),
            (BQError::InvalidColumnIndex { col_index: 1 }, ErrorKind::InvalidData),
            (
                BQError::InvalidColumnName { col_name: "missing_column".to_owned() },
                ErrorKind::InvalidData,
            ),
            (BQError::NoDataAvailable, ErrorKind::InvalidState),
            (BQError::NoToken, ErrorKind::DestinationAuthenticationError),
            (
                BQError::SerializationError(serde_json::from_str::<Value>("{").unwrap_err()),
                ErrorKind::SerializationError,
            ),
        ] {
            assert_eq!(provider_error.retryability(), Retryability::Permanent);
            let provider_type = std::mem::discriminant(&provider_error);
            let error = bq_error_to_etl_error(provider_error);
            let mut attempts = 0;

            let returned = retry_query_jobs(QueryRequest::new("select 1"), |_, _| {
                attempts += 1;
                std::future::ready(Err(error.clone()))
            })
            .await
            .unwrap_err();

            assert_eq!(attempts, 1);
            assert_eq!(returned.kind(), kind);
            assert_eq!(
                std::mem::discriminant(
                    returned.source().unwrap().downcast_ref::<BQError>().unwrap()
                ),
                provider_type
            );
        }
    }

    /// Terminal failures rotate job identities; other failures must not replay
    /// DDL.
    #[tokio::test(start_paused = true)]
    async fn query_job_retries_preserve_deadline_and_rotate_only_failed_jobs() {
        for (reason, failures, expected_attempts) in [
            ("jobBackendError", 1, 2),
            ("jobInternalError", 1, 2),
            ("jobRateLimitExceeded", 10, 5),
            ("invalidQuery", 1, 1),
            ("backendError", 1, 1),
        ] {
            let mut request = QueryRequest::new("select 1");
            request.request_id = Some("caller-request-id".to_owned());
            let mut request_ids = Vec::new();
            let deadline = Instant::now() + QUERY_TIMEOUT;

            let result = retry_query_jobs(request, |request, attempt_deadline| {
                assert_eq!(attempt_deadline, deadline);
                assert_eq!(request.timeout_ms, Some(QUERY_WAIT_TIMEOUT_MS));
                request_ids.push(request.request_id.unwrap());
                let fail = request_ids.len() <= failures;

                async move {
                    if fail {
                        Err(bq_error_to_etl_error(query_response_error(400, reason)))
                    } else {
                        Ok(QueryResponse { job_complete: Some(true), ..Default::default() })
                    }
                }
            })
            .await;

            assert_eq!(request_ids.len(), expected_attempts, "{reason}");
            assert_eq!(request_ids[0], "caller-request-id");
            for pair in request_ids.windows(2) {
                assert_ne!(pair[0], pair[1]);
                assert_eq!(pair[1].len(), 32);
                assert!(pair[1].bytes().all(|byte| byte.is_ascii_hexdigit()));
            }
            assert_eq!(result.is_ok(), failures < expected_attempts, "{reason}");
        }
    }

    /// Backoff must meet the minimum delay and remain within the service cap.
    #[test]
    fn query_backoff_respects_minimum_and_cap() {
        for seconds in [1, 2, 4, 8, 16, 32] {
            let base = Duration::from_secs(seconds);
            for _ in 0..100 {
                let delay = query_retry_delay(base);
                assert!(delay >= base);
                assert!(delay <= (base + Duration::from_secs(1)).min(QUERY_MAX_RETRY_DELAY));
            }
        }
    }
}
