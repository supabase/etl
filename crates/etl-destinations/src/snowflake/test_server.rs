//! Scripted HTTP server and token provider shared by Snowflake client unit
//! tests.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use crate::snowflake::{Result, auth::TokenProvider};

/// Token provider that names each token after how often it was invalidated.
#[derive(Default)]
pub(crate) struct CountingTokenProvider {
    /// Number of invalidations so far.
    invalidations: AtomicUsize,
}

impl CountingTokenProvider {
    /// Returns how often the token was invalidated.
    pub(crate) fn invalidations(&self) -> usize {
        self.invalidations.load(Ordering::SeqCst)
    }
}

impl TokenProvider for CountingTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok(format!("token-{}", self.invalidations()))
    }

    async fn invalidate_token(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

/// Answers one connection per scripted `(status, body)` response.
///
/// Returns the server URL and a handle that yields the bearer token of each
/// answered request.
pub(crate) async fn serve_responses(
    responses: Vec<(u16, &'static str)>,
) -> (String, JoinHandle<Vec<String>>) {
    serve_requests(
        responses.into_iter().map(|(status, body)| (status, body.to_owned())).collect(),
        |request, head_end| {
            std::str::from_utf8(&request[..head_end]).unwrap().lines().find_map(|line| {
                line.to_ascii_lowercase().strip_prefix("authorization: bearer ").map(str::to_owned)
            })
        },
    )
    .await
}

/// Answers SQL requests and records the statements, including unexpected DDL.
pub(crate) async fn serve_sql_responses(
    responses: Vec<String>,
) -> (String, JoinHandle<Vec<String>>) {
    serve_requests(responses.into_iter().map(|body| (200, body)).collect(), |request, head_end| {
        let body: serde_json::Value = serde_json::from_slice(&request[head_end..]).unwrap();
        Some(body["statement"].as_str().unwrap().to_owned())
    })
    .await
}

/// Reads complete requests before recording them and returning scripted
/// replies.
async fn serve_requests(
    responses: Vec<(u16, String)>,
    record: impl Fn(&[u8], usize) -> Option<String> + Send + 'static,
) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();

        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 1024];
            let head_end = loop {
                if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    break index + 4;
                }

                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            };

            let content_length = std::str::from_utf8(&request[..head_end])
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() < head_end + content_length {
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }

            requests.extend(record(&request, head_end));
            let response = format!(
                "HTTP/1.1 {status} Test\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }

        requests
    });

    (url, server)
}
