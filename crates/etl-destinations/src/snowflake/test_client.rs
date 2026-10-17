//! Local SQL API fixtures for Snowflake lifecycle tests.

use std::sync::Arc;

use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_util::task::AbortOnDropHandle;

use crate::snowflake::{Config, Result, SqlClient, auth::TokenProvider};

/// Supplies an inert token to loopback SQL API requests.
pub(super) struct TestTokenProvider;

impl TokenProvider for TestTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok("placeholder-token".to_owned())
    }

    async fn invalidate_token(&self) {}
}

/// Serves a finite SQL response script and records each submitted statement.
pub(super) async fn sql_fixture(
    responses: Vec<Value>,
) -> (SqlClient<TestTokenProvider>, AbortOnDropHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());

    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut statements = Vec::new();

        for body in responses {
            let (stream, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(stream).lines();
            let request_line = lines.next_line().await.unwrap().unwrap();
            assert!(request_line.starts_with("POST /api/v2/statements"));
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
            let request: Value = serde_json::from_slice(&bytes).unwrap();
            statements.push(request["statement"].as_str().unwrap().to_owned());

            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 200 Fixture\r\ncontent-type: application/json\r\ncontent-length: \
                 {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            reader.get_mut().write_all(response.as_bytes()).await.unwrap();
        }

        statements
    }));

    let config = Config::new("example-account", "test-user", "test-db", "test-schema")
        .unwrap()
        .with_test_server(&base_url);
    let sql = SqlClient::new(
        config,
        Arc::new(TestTokenProvider),
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );

    (sql, server)
}
