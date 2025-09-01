use actix_web::{Error, dev::ServiceRequest, web::Data};
use actix_web_httpauth::extractors::{
    AuthenticationError,
    bearer::{BearerAuth, Config},
};
use constant_time_eq::constant_time_eq_n;

use crate::config::{ApiConfig, ApiKey};

/// Validates bearer token authentication for API requests.
///
/// Compares the provided token against the configured API key using constant-time
/// comparison to prevent timing attacks. Returns authentication errors for invalid tokens.
pub async fn auth_validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let config = req
        .app_data::<Config>()
        .cloned()
        .unwrap_or_default()
        .scope("v1");

    let api_config = req
        .app_data::<Data<ApiConfig>>()
        .expect("Missing API configuration while doing authentication");

    let token = credentials.token();
    let token: ApiKey = match token.try_into() {
        Ok(token) => token,
        Err(_) => {
            return Err((AuthenticationError::from(config).into(), req));
        }
    };

    // Decode all configured API keys (rotation supported via multiple entries).
    let configured_keys: Vec<ApiKey> = {
        let keys = &api_config.api_keys;
        if keys.is_empty() {
            return Err((AuthenticationError::from(config).into(), req));
        }

        let mut configured_keys = Vec::with_capacity(keys.len());
        for key in keys {
            match key.as_str().try_into() {
                Ok(k) => configured_keys.push(k),
                Err(_) => return Err((AuthenticationError::from(config).into(), req)),
            }
        }

        configured_keys
    };

    // Compare against all configured keys without an early exit to avoid timing leaks.
    let mut valid = false;
    for key in &configured_keys {
        valid |= constant_time_eq_n(&key.key, &token.key);
    }

    if !valid {
        return Err((AuthenticationError::from(config).into(), req));
    }

    Ok(req)
}
