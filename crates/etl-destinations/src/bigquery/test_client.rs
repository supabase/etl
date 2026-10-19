//! HTTP fixtures for testing BigQuery clients without remote API calls.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use gcp_bigquery_client::{
    Client, auth::Authenticator, client_builder::ClientBuilder, error::BQError,
};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_util::task::AbortOnDropHandle;

/// Counts request authentication and supplies an inert fixture token.
#[derive(Clone, Default)]
pub(super) struct TestAuthenticator {
    /// Number of authentication requests made through this fixture.
    calls: Arc<AtomicUsize>,
}

impl TestAuthenticator {
    /// Returns the number of authentication requests observed so far.
    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Authenticator for TestAuthenticator {
    async fn access_token(&self) -> Result<String, BQError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok("placeholder-token".to_owned())
    }
}

/// Serves a finite response script and captures requests through the real
/// BigQuery SDK, including JSON bodies and result-page parameters.
pub(super) async fn http_fixture(
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
                "HTTP/1.1 {status} Fixture\r\ncontent-type: application/json\r\ncontent-length: \
                 {}\r\nconnection: close\r\n\r\n{body}",
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
