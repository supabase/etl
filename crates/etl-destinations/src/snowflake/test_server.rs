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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut tokens = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut chunk = [0; 1024];
            while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                head.extend_from_slice(&chunk[..read]);
            }
            let head = String::from_utf8(head).unwrap();
            tokens.extend(head.lines().find_map(|line| {
                line.to_ascii_lowercase().strip_prefix("authorization: bearer ").map(str::to_owned)
            }));
            let response = format!(
                "HTTP/1.1 {status} Test\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        tokens
    });

    (url, server)
}
