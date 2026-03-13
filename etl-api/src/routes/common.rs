use crate::config::ApiConfig;
use crate::configs::source::StoredSourceConfig;
use crate::k8s::TrustedRootCertsCache;
use crate::validation::{self, ValidationContext, ValidationError, ValidationFailure};

/// Validates a source config against the trusted source profile, when enabled.
pub async fn validate_source_config(
    source_config: StoredSourceConfig,
    api_config: &ApiConfig,
    trusted_root_certs_cache: &TrustedRootCertsCache,
) -> Result<Vec<ValidationFailure>, ValidationError> {
    if api_config.source.trusted_username.is_none() {
        return Ok(vec![]);
    }

    let ctx =
        ValidationContext::build_from_source(source_config, api_config, trusted_root_certs_cache)
            .await?;
    validation::validate_source(&ctx).await
}
