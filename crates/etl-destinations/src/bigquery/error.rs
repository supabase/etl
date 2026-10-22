//! Redacted BigQuery diagnostics with access to typed errors for recovery.

use std::{error::Error, fmt};

use etl::error::Retryability;
use gcp_bigquery_client::{
    error::BQError,
    yup_oauth2::{
        Error as OAuthError,
        error::{AuthError, AuthErrorCode},
    },
};

use crate::{
    bigquery::client::{TRANSIENT_BIGQUERY_RESPONSE_REASONS, decode_storage_error_codes},
    retry::ClassifiedError,
};

/// Additional REST reason codes safe to include in diagnostics.
const KNOWN_BIGQUERY_RESPONSE_REASONS: &[&str] = &[
    "accessDenied",
    "duplicate",
    "invalid",
    "invalidQuery",
    "notFound",
    "quotaExceeded",
    "resourcesExceeded",
    "timeout",
    "tooManyRequests",
];

/// Preserves known OAuth codes while omitting unrecognized provider strings.
fn oauth_error_summary(error: &AuthError) -> String {
    let code = match &error.error {
        AuthErrorCode::Other(_) => "unknown",
        code => code.as_str(),
    };
    format!("BigQuery OAuth {code} error; provider details omitted")
}

/// Describes a provider error without formatting its messages or sources.
///
/// HTTP and gRPC codes remain available, but response text, URLs, OAuth
/// details, serialization input, and nested transport errors are omitted.
pub(super) fn bigquery_error_summary(error: &BQError) -> String {
    let category = match error {
        BQError::ResponseError { error } => {
            let mut reasons: Vec<_> = error
                .error
                .errors
                .iter()
                .filter_map(|error| error.get("reason"))
                .map(|reason| {
                    TRANSIENT_BIGQUERY_RESPONSE_REASONS
                        .iter()
                        .chain(KNOWN_BIGQUERY_RESPONSE_REASONS)
                        .copied()
                        .find(|known| known == reason)
                        .unwrap_or("unknown")
                })
                .collect();
            reasons.sort_unstable();
            reasons.dedup();
            let detail = if reasons.is_empty() {
                String::new()
            } else {
                format!(" (reasons: {})", reasons.join(", "))
            };
            return format!(
                "BigQuery HTTP {} error{detail}; provider details omitted",
                error.error.code
            );
        }
        BQError::TonicStatusError(status) => {
            let codes = decode_storage_error_codes(status);
            let detail = if codes.is_empty() {
                String::new()
            } else {
                format!(" (storage_error_codes={})", codes.join(","))
            };
            return format!(
                "BigQuery gRPC {:?} error{detail}; provider details omitted",
                status.code()
            );
        }
        BQError::RequestError(error) => {
            let category = if error.is_timeout() {
                "request timeout"
            } else if error.is_connect() {
                "connection error"
            } else if error.is_builder() {
                "request configuration error"
            } else if error.is_redirect() {
                "redirect error"
            } else if error.is_decode() {
                "response decoding error"
            } else {
                "request error"
            };
            return format!(
                "BigQuery {category} (HTTP status: {:?}); provider details omitted",
                error.status().map(|status| status.as_u16())
            );
        }
        BQError::InvalidServiceAccountKey(_) => "invalid service account key",
        BQError::InvalidServiceAccountAuthenticator(_) => "invalid service account authenticator",
        BQError::InvalidInstalledFlowAuthenticator(_) => "invalid installed flow authenticator",
        BQError::InvalidApplicationDefaultCredentialsAuthenticator(_) => {
            "invalid application default credentials authenticator"
        }
        BQError::InvalidAuthorizedUserAuthenticator(_) => "invalid authorized user authenticator",
        BQError::AuthError(error) | BQError::YupAuthError(OAuthError::AuthError(error)) => {
            return oauth_error_summary(error);
        }
        BQError::YupAuthError(_) => "authentication error",
        BQError::NoToken => "missing authentication token",
        BQError::NoDataAvailable => "result set positioning error",
        BQError::InvalidColumnIndex { .. } => "invalid column index",
        BQError::InvalidColumnName { .. } => "invalid column name",
        BQError::InvalidColumnType { .. } => "column type mismatch",
        BQError::SerializationError(_) => "JSON serialization error",
        BQError::TonicTransportError(_) => "transport error",
        BQError::TonicInvalidMetadataValueError(_) => "invalid metadata value",
        BQError::SemaphorePermitError(_) => "semaphore permit error",
        BQError::TokioTaskError(_) => "task execution error",
        BQError::ConnectionPoolError(_) => "connection pool error",
    };

    format!("BigQuery {category}; provider details omitted")
}

/// Keeps provider errors available for classification without exposing them
/// through diagnostics or the public error source chain.
pub(super) struct RedactedBigQueryError(BQError);

impl RedactedBigQueryError {
    /// Returns the original error for internal retry and recovery decisions.
    /// It must not be formatted or exposed through an error source chain.
    pub(super) fn inner(&self) -> &BQError {
        &self.0
    }
}

impl From<BQError> for RedactedBigQueryError {
    fn from(error: BQError) -> Self {
        Self(error)
    }
}

impl fmt::Display for RedactedBigQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&bigquery_error_summary(&self.0))
    }
}

impl fmt::Debug for RedactedBigQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Error for RedactedBigQueryError {}

impl ClassifiedError for RedactedBigQueryError {
    fn retryability(&self) -> Retryability {
        self.0.retryability()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use etl::error::Retryability;
    use gcp_bigquery_client::{
        error::{BQError, NestedResponseError, ResponseError},
        yup_oauth2::{
            error::{AuthError, AuthErrorCode},
            parse_service_account_key,
        },
    };

    use crate::{
        bigquery::{
            client::bq_error_to_etl_error,
            error::{RedactedBigQueryError, bigquery_error_summary},
        },
        retry::ClassifiedError,
    };

    /// Provider text cannot escape through either formatting mode or a nested
    /// source, while the original typed error remains available internally.
    #[test]
    fn bigquery_errors_redact_credentials_and_preserve_classification() {
        let key_error = parse_service_account_key(br#""placeholder-private-key""#).unwrap_err();
        let auth_error = AuthError {
            error: AuthErrorCode::Other("placeholder-oauth-code".to_owned()),
            error_description: Some("placeholder-access-token".to_owned()),
            error_uri: Some("https://example.com/?token=placeholder-uri-token".to_owned()),
        };
        let response_error = BQError::ResponseError {
            error: ResponseError {
                error: NestedResponseError {
                    code: 403,
                    errors: vec![
                        [
                            ("reason".to_owned(), "rateLimitExceeded".to_owned()),
                            ("message".to_owned(), "placeholder-response-token".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ],
                    message: "placeholder-response-body".to_owned(),
                    status: "placeholder-response-status".to_owned(),
                },
            },
        };
        let status = tonic::Status::with_details(
            tonic::Code::InvalidArgument,
            "schema_mismatch_extra_fields placeholder-row-value",
            b"placeholder-grpc-details".as_slice().into(),
        );

        for raw in [
            BQError::InvalidServiceAccountKey(key_error),
            BQError::AuthError(auth_error),
            response_error,
            BQError::TonicStatusError(status),
            BQError::ConnectionPoolError("placeholder-pool-token".to_owned()),
        ] {
            assert!(format!("{raw:?}").contains("placeholder-"));
            let expected_retryability = raw.retryability();
            let expected_type = std::mem::discriminant(&raw);
            let summary = bigquery_error_summary(&raw);
            assert!(!summary.contains("placeholder-"));

            let error = bq_error_to_etl_error(raw);

            assert_eq!(error.retryability(), expected_retryability);
            assert!(!error.to_string().contains("placeholder-"));
            assert!(!format!("{error:?}").contains("placeholder-"));
            let source = error.source().unwrap();
            assert_eq!(source.to_string(), summary);
            assert_eq!(format!("{source:?}"), summary);
            assert!(source.source().is_none());
            let redacted = source.downcast_ref::<RedactedBigQueryError>().unwrap();
            assert_eq!(std::mem::discriminant(redacted.inner()), expected_type);
        }
    }

    /// REST reasons distinguish failed jobs, rate limits, and permanent
    /// failures without accepting arbitrary provider strings as codes.
    #[test]
    fn bigquery_error_summaries_allowlist_response_reasons() {
        for reason in ["jobBackendError", "rateLimitExceeded", "quotaExceeded", "invalidQuery"] {
            let error = BQError::ResponseError {
                error: ResponseError {
                    error: NestedResponseError {
                        code: 403,
                        errors: vec![
                            [("reason".to_owned(), reason.to_owned())].into_iter().collect(),
                            [("reason".to_owned(), "placeholder-secret-reason".to_owned())]
                                .into_iter()
                                .collect(),
                        ],
                        message: "placeholder-provider-message".to_owned(),
                        status: "placeholder-provider-status".to_owned(),
                    },
                },
            };

            let error = bq_error_to_etl_error(error);

            assert!(error.to_string().contains(reason));
            assert!(error.to_string().contains("unknown"));
            assert!(!error.to_string().contains("placeholder-"));
            assert!(!format!("{error:?}").contains("placeholder-"));
        }
    }

    /// Redaction must retain the protocol codes used to diagnose failures.
    #[test]
    fn bigquery_error_summaries_keep_protocol_codes() {
        let raw = BQError::TonicStatusError(tonic::Status::unavailable("placeholder-token"));
        let error = RedactedBigQueryError::from(raw);
        assert!(error.to_string().contains("Unavailable"));
        assert_eq!(error.retryability(), Retryability::Retryable);
    }
}
