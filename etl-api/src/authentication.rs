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

    let api_key = req
        .app_data::<Data<ApiConfig>>()
        .expect("missing api configuration")
        .api_key
        .as_str();

    let token = credentials.token();

    let api_key: ApiKey = match api_key.try_into() {
        Ok(api_key) => api_key,
        Err(_) => {
            return Err((AuthenticationError::from(config).into(), req));
        }
    };

    let token: ApiKey = match token.try_into() {
        Ok(token) => token,
        Err(_) => {
            return Err((AuthenticationError::from(config).into(), req));
        }
    };

    if !constant_time_eq_n(&api_key.key, &token.key) {
        return Err((AuthenticationError::from(config).into(), req));
    }

    Ok(req)
}
