use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use constant_time_eq::constant_time_eq_n;

use crate::config::{ApiConfig, ApiKey};

/// Authentication failure for protected API routes.
pub struct AuthError;

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let mut response = StatusCode::UNAUTHORIZED.into_response();
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer scope=\"v1\""));
        response
    }
}

/// Validates bearer token authentication for API requests.
///
/// Compares the provided token against the configured API key using
/// constant-time comparison to prevent timing attacks. Returns authentication
/// errors for invalid tokens.
pub async fn auth_validator(
    State(api_config): State<Arc<ApiConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AuthError> {
    let token = bearer_token(request.headers())?;
    let token: ApiKey = token.try_into().map_err(|_| AuthError)?;

    // Decode all configured API keys (rotation supported via multiple entries).
    let configured_keys: Vec<ApiKey> = {
        let keys = &api_config.api_keys;
        if keys.is_empty() {
            return Err(AuthError);
        }

        let mut configured_keys = Vec::with_capacity(keys.len());
        for key in keys {
            match key.as_str().try_into() {
                Ok(k) => configured_keys.push(k),
                Err(_) => return Err(AuthError),
            }
        }

        configured_keys
    };

    // Compare against all configured keys without an early exit to avoid timing
    // leaks.
    let mut valid = false;
    for key in &configured_keys {
        valid |= constant_time_eq_n(&key.key, &token.key);
    }

    if !valid {
        return Err(AuthError);
    }

    Ok(next.run(request).await)
}

/// Extracts the bearer token from request headers.
fn bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let header = headers.get(AUTHORIZATION).ok_or(AuthError)?;
    let header = header.to_str().map_err(|_| AuthError)?;
    let (scheme, token) = header.split_once(' ').ok_or(AuthError)?;

    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(AuthError);
    }

    Ok(token)
}
