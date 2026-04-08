use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use gcp_bigquery_client::auth::Authenticator;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::yup_oauth2::authenticator::{
    ApplicationDefaultCredentialsTypes, Authenticator as YupAuthenticator,
};
use gcp_bigquery_client::yup_oauth2::authorized_user::AuthorizedUserSecret;
use gcp_bigquery_client::yup_oauth2::hyper_rustls::HttpsConnector;
use gcp_bigquery_client::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator as YupApplicationDefaultCredentialsAuthenticator,
    ApplicationDefaultCredentialsFlowOpts,
    AuthorizedUserAuthenticator as YupAuthorizedUserAuthenticator,
    InstalledFlowAuthenticator as YupInstalledFlowAuthenticator, InstalledFlowReturnMethod,
    ServiceAccountKey,
};
use hyper_util::client::legacy::connect::HttpConnector;

const BIGQUERY_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

pub(super) type BigQueryAuthenticator = Arc<dyn Authenticator>;

/// Authenticates BigQuery writes with a service account.
#[derive(Clone)]
struct ServiceAccountTokenProvider {
    authenticator: YupAuthenticator<HttpsConnector<HttpConnector>>,
    scopes: Vec<String>,
}

#[async_trait]
impl Authenticator for ServiceAccountTokenProvider {
    async fn access_token(&self) -> Result<String, BQError> {
        Ok(self
            .authenticator
            .clone()
            .token(self.scopes.as_ref())
            .await?
            .token()
            .ok_or(BQError::NoToken)?
            .to_string())
    }
}

/// Authenticates BigQuery writes with an installed flow.
#[derive(Clone)]
struct InstalledFlowTokenProvider {
    authenticator: YupAuthenticator<HttpsConnector<HttpConnector>>,
    scopes: Vec<String>,
}

#[async_trait]
impl Authenticator for InstalledFlowTokenProvider {
    async fn access_token(&self) -> Result<String, BQError> {
        Ok(self
            .authenticator
            .clone()
            .token(self.scopes.as_ref())
            .await?
            .token()
            .ok_or(BQError::NoToken)?
            .to_string())
    }
}

/// Authenticates BigQuery writes with application default credentials.
#[derive(Clone)]
struct ApplicationDefaultCredentialsTokenProvider {
    authenticator: YupAuthenticator<HttpsConnector<HttpConnector>>,
    scopes: Vec<String>,
}

#[async_trait]
impl Authenticator for ApplicationDefaultCredentialsTokenProvider {
    async fn access_token(&self) -> Result<String, BQError> {
        Ok(self
            .authenticator
            .clone()
            .token(self.scopes.as_ref())
            .await?
            .token()
            .ok_or(BQError::NoToken)?
            .to_string())
    }
}

/// Authenticates BigQuery writes with authorized user credentials.
#[derive(Clone)]
struct AuthorizedUserTokenProvider {
    authenticator: YupAuthenticator<HttpsConnector<HttpConnector>>,
    scopes: Vec<String>,
}

#[async_trait]
impl Authenticator for AuthorizedUserTokenProvider {
    async fn access_token(&self) -> Result<String, BQError> {
        Ok(self
            .authenticator
            .clone()
            .token(self.scopes.as_ref())
            .await?
            .token()
            .ok_or(BQError::NoToken)?
            .to_string())
    }
}

/// Creates a BigQuery authenticator from a service account key file.
pub(super) async fn service_account_key_file_authenticator(
    service_account_key_file: &str,
) -> Result<BigQueryAuthenticator, BQError> {
    let service_account_key =
        gcp_bigquery_client::yup_oauth2::read_service_account_key(service_account_key_file)
            .await
            .map_err(BQError::InvalidServiceAccountKey)?;

    service_account_key_authenticator(service_account_key).await
}

/// Creates a BigQuery authenticator from a parsed service account key.
pub(super) async fn service_account_key_authenticator(
    service_account_key: ServiceAccountKey,
) -> Result<BigQueryAuthenticator, BQError> {
    let authenticator =
        gcp_bigquery_client::yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key)
            .build()
            .await
            .map_err(BQError::InvalidServiceAccountAuthenticator)?;

    Ok(Arc::new(ServiceAccountTokenProvider {
        authenticator,
        scopes: vec![BIGQUERY_SCOPE.to_string()],
    }))
}

/// Creates a BigQuery authenticator using an installed OAuth flow.
pub(super) async fn installed_flow_authenticator<S, P>(
    secret: S,
    persistent_file_path: P,
) -> Result<BigQueryAuthenticator, BQError>
where
    S: AsRef<[u8]>,
    P: Into<PathBuf>,
{
    let app_secret = gcp_bigquery_client::yup_oauth2::parse_application_secret(secret)?;
    let authenticator =
        YupInstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::HTTPRedirect)
            .persist_tokens_to_disk(persistent_file_path)
            .build()
            .await
            .map_err(BQError::InvalidInstalledFlowAuthenticator)?;

    authenticator
        .token(&[BIGQUERY_SCOPE])
        .await
        .map_err(BQError::YupAuthError)?;

    Ok(Arc::new(InstalledFlowTokenProvider {
        authenticator,
        scopes: vec![BIGQUERY_SCOPE.to_string()],
    }))
}

/// Creates a BigQuery authenticator from application default credentials.
pub(super) async fn application_default_credentials_authenticator()
-> Result<BigQueryAuthenticator, BQError> {
    if let Some(authenticator) = try_load_authorized_user_credentials().await? {
        return Ok(authenticator);
    }

    let opts = ApplicationDefaultCredentialsFlowOpts::default();
    let authenticator = match YupApplicationDefaultCredentialsAuthenticator::builder(opts).await {
        ApplicationDefaultCredentialsTypes::InstanceMetadata(authenticator) => {
            authenticator.build().await
        }
        ApplicationDefaultCredentialsTypes::ServiceAccount(authenticator) => {
            authenticator.build().await
        }
    }
    .map_err(BQError::InvalidApplicationDefaultCredentialsAuthenticator)?;

    Ok(Arc::new(ApplicationDefaultCredentialsTokenProvider {
        authenticator,
        scopes: vec![BIGQUERY_SCOPE.to_string()],
    }))
}

/// Returns the default application default credentials path.
fn default_adc_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        env::var("APPDATA").ok().map(|appdata| {
            PathBuf::from(appdata).join("gcloud/application_default_credentials.json")
        })
    }

    #[cfg(not(target_os = "windows"))]
    {
        env::var("HOME").ok().map(|home| {
            PathBuf::from(home).join(".config/gcloud/application_default_credentials.json")
        })
    }
}

/// Returns the configured application default credentials path.
fn adc_credential_path() -> Option<PathBuf> {
    env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .map(PathBuf::from)
        .or_else(default_adc_path)
}

/// Detects the credential type stored at the ADC path.
#[derive(serde::Deserialize)]
struct CredentialType {
    #[serde(rename = "type")]
    credential_type: String,
}

/// Loads authorized user credentials from the ADC path when present.
async fn try_load_authorized_user_credentials() -> Result<Option<BigQueryAuthenticator>, BQError> {
    let credential_path = match adc_credential_path() {
        Some(path) => path,
        None => return Ok(None),
    };
    let contents = match tokio::fs::read_to_string(&credential_path).await {
        Ok(contents) => contents,
        Err(_) => return Ok(None),
    };
    let credential_type: CredentialType = match serde_json::from_str(&contents) {
        Ok(credential_type) => credential_type,
        Err(_) => return Ok(None),
    };

    if credential_type.credential_type != "authorized_user" {
        return Ok(None);
    }

    let secret = serde_json::from_str::<AuthorizedUserSecret>(&contents).map_err(|error| {
        BQError::InvalidAuthorizedUserAuthenticator(std::io::Error::other(error.to_string()))
    })?;
    let authenticator = YupAuthorizedUserAuthenticator::builder(secret)
        .build()
        .await
        .map_err(BQError::InvalidAuthorizedUserAuthenticator)?;

    Ok(Some(Arc::new(AuthorizedUserTokenProvider {
        authenticator,
        scopes: vec![BIGQUERY_SCOPE.to_string()],
    })))
}
