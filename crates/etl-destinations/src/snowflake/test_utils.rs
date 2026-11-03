use crate::snowflake::{Config, Result, auth::TokenProvider, sql_client::SqlClient};

/// Generates deterministic ASCII alphanumeric text from `seed`.
pub fn random_text(len: usize, seed: u64) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ALPHABET[((state >> 33) % 62) as usize] as char
        })
        .collect()
}

/// Builds deterministic padded ASCII records, truncated to `len` bytes.
///
/// Each record contains `random_len` pseudo-random alphanumeric characters.
/// Use `1..=230` for 256-byte records; 88 characters compress about 3.8x,
/// and 40 about 6x. Inputs must be small enough for allocation sizes to fit.
///
/// # Panics
///
/// Panics if `random_len` is zero.
pub fn structured_text(len: usize, seed: u64, random_len: usize) -> String {
    let random = random_text((len / 256 + 1) * random_len, seed);
    let mut payload = String::with_capacity(len + 256);
    for chunk in random.as_bytes().chunks_exact(random_len) {
        let start = payload.len();
        payload.push_str("{\"token\":\"");
        payload.push_str(std::str::from_utf8(chunk).unwrap());
        payload.push_str("\",\"padding\":\"");
        while payload.len() < start + 253 {
            payload.push('0');
        }
        payload.push_str("\"}\n");
        if payload.len() >= len {
            break;
        }
    }
    payload.truncate(len);
    payload
}

/// Load a [`Config`] from environment variables for integration tests.
pub fn load_test_config() -> Config {
    Config::require_tests_env().unwrap_or_else(|error| panic!("{error}"))
}

/// Executes a SQL statement and returns its result rows.
///
/// Data queries require an active warehouse, while metadata commands such as
/// `SHOW` do not.
pub async fn query_rows<T: TokenProvider>(
    client: &SqlClient<T>,
    sql: &str,
) -> Result<Vec<Vec<serde_json::Value>>> {
    let resp = client.execute_statement(sql).await?;
    Ok(resp.data.unwrap_or_default())
}
