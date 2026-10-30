//! Platform front-door requests for ordinary hosted pipelines.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Method};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use tokio::{sync::Mutex, time::Instant};

use crate::plan::Plan;

/// Authenticated project-scoped Platform client. Never serializes
/// authentication or provider bodies.
#[derive(Clone)]
pub(crate) struct Platform {
    http: Client,
    base: String,
    auth: Arc<Authentication>,
}

impl Platform {
    /// Loads the full staging replication endpoint, including its project
    /// reference.
    pub(crate) fn from_env(plan: &Plan) -> Result<Self> {
        ensure!(
            std::env::var("EXPERIMENT_STAGING_PROJECT_REF").ok().as_deref()
                == Some(plan.project_ref.as_str()),
            "Staging project allowlist does not match the plan"
        );
        let base = std::env::var("EXPERIMENT_PLATFORM_URL")
            .context("EXPERIMENT_PLATFORM_URL is required")?;
        let url = reqwest::Url::parse(&base).context("Invalid Platform URL")?;
        ensure!(
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Platform requires a credential-free HTTPS endpoint"
        );
        ensure!(
            url.host_str() != Some("api.supabase.com"),
            "Production Platform endpoint is forbidden"
        );
        ensure!(
            url.path()
                .trim_end_matches('/')
                .ends_with(&format!("/replication/{}", plan.project_ref)),
            "Platform endpoint must select the planned project"
        );
        Ok(Self {
            http: Client::builder()
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base: base.trim_end_matches('/').to_owned(),
            auth: Arc::new(Authentication::from_env()?),
        })
    }

    /// Performs a single mutation or read; ambiguous mutations are never
    /// blindly retried.
    pub(crate) async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        let absent_is_success = method == Method::GET || method == Method::DELETE;
        let token = self.auth.token(&self.http).await?;
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(token.expose_secret());
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|_| {
            anyhow::anyhow!(
                "Platform transport failed; reconcile resource inventory before retrying"
            )
        })?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND && absent_is_success {
            return Ok(Value::Null);
        }
        ensure!(status.is_success(), "Platform request failed with HTTP {}", status.as_u16());
        let bytes =
            response.bytes().await.map_err(|_| anyhow::anyhow!("Platform response read failed"))?;
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes).context("Invalid Platform response structure")
    }

    /// Resolves the project's managed source without creating duplicates.
    pub(crate) async fn source(&self) -> Result<u64> {
        let result = self.request(Method::GET, "/sources", None).await?;
        if result.is_null() {
            let created = self.request(Method::POST, "/tenants-sources", Some(json!({}))).await?;
            return created["source_id"].as_u64().context("Missing created source ID");
        }
        let sources = result["sources"].as_array().context("Missing sources array")?;
        match sources.as_slice() {
            [source] => id(source),
            [] => {
                let created =
                    self.request(Method::POST, "/tenants-sources", Some(json!({}))).await?;
                created["source_id"].as_u64().context("Missing created source ID")
            }
            _ => bail!("Project has multiple sources; use a dedicated experiment project"),
        }
    }

    /// Reads the immutable IDs and configuration needed to validate ownership.
    pub(crate) async fn pipeline(&self, pipeline: u64) -> Result<Value> {
        self.request(Method::GET, &format!("/pipelines/{pipeline}"), None).await
    }

    /// Starts or stops the selected ordinary pipeline.
    pub(crate) async fn action(&self, pipeline: u64, action: &str) -> Result<()> {
        ensure!(matches!(action, "start" | "stop" | "restart"), "Invalid pipeline action");
        self.request(Method::POST, &format!("/pipelines/{pipeline}/{action}"), Some(json!({})))
            .await?;
        Ok(())
    }

    /// Waits for the asynchronous runtime stop to complete.
    pub(crate) async fn stop(&self, pipeline: u64) -> Result<()> {
        let status =
            self.request(Method::GET, &format!("/pipelines/{pipeline}/status"), None).await?;
        if status.is_null() || is_stopped(&status) {
            return Ok(());
        }
        self.action(pipeline, "stop").await?;
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let status = self
                    .request(Method::GET, &format!("/pipelines/{pipeline}/status"), None)
                    .await?;
                if is_stopped(&status) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
        .await
        .context("Pipeline stop was not confirmed before timeout")?
    }

    /// Reclaims the ordinary runtime and slots after the caller has verified
    /// run ownership.
    pub(crate) async fn reclaim(&self, pipeline: u64) -> Result<()> {
        self.stop(pipeline).await?;
        self.request(Method::DELETE, &format!("/pipelines/{pipeline}"), None).await?;
        Ok(())
    }

    /// Returns only structural table states, avoiding provider error messages
    /// in artifacts.
    pub(crate) async fn replication(&self, pipeline: u64) -> Result<Value> {
        let value = self
            .request(Method::GET, &format!("/pipelines/{pipeline}/replication-status"), None)
            .await?;
        // Before the first replicator start, the API returns 404 while ETL
        // metadata is absent. The lifecycle owner separately validates
        // existence.
        if value.is_null() {
            return Ok(json!({"tables":[],"state_initialized":false}));
        }
        let tables = value["table_statuses"].as_array().context("Missing table statuses")?;
        Ok(
            json!({"tables": tables.iter().map(|t| json!({"id":t["id"],"schema":t["schema"],"name":t["name"],"state":t["state"]["name"],"slot":t["table_sync_lag"]})).collect::<Vec<_>>(), "apply_lag":value["apply_lag"]}),
        )
    }
}

/// Reads the API's tagged runtime status, shared with Studio.
pub(crate) fn is_stopped(value: &Value) -> bool {
    value["status"]["name"] == "stopped"
}

/// Extracts a positive API resource ID.
pub(crate) fn id(value: &Value) -> Result<u64> {
    value["id"].as_u64().filter(|id| *id > 0).context("Missing resource ID")
}

/// Dedicated staging identity; each process obtains its own renewable session.
struct Authentication {
    url: String,
    api_key: SecretString,
    email: SecretString,
    password: SecretString,
    cached: Mutex<Option<(SecretString, Instant)>>,
}

impl Authentication {
    /// Rejects browser-session/PAT shortcuts which would expire during a long
    /// run.
    fn from_env() -> Result<Self> {
        let url =
            std::env::var("EXPERIMENT_AUTH_URL").context("EXPERIMENT_AUTH_URL is required")?;
        let parsed = reqwest::Url::parse(&url).context("Invalid staging authentication URL")?;
        ensure!(
            parsed.scheme() == "https"
                && parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.query().is_none(),
            "Authentication requires a credential-free HTTPS URL"
        );
        Ok(Self {
            url: url.trim_end_matches('/').to_owned(),
            api_key: std::env::var("EXPERIMENT_AUTH_API_KEY")
                .context("EXPERIMENT_AUTH_API_KEY is required")?
                .into(),
            email: std::env::var("EXPERIMENT_PLATFORM_EMAIL")
                .context("EXPERIMENT_PLATFORM_EMAIL is required")?
                .into(),
            password: std::env::var("EXPERIMENT_PLATFORM_PASSWORD")
                .context("EXPERIMENT_PLATFORM_PASSWORD is required")?
                .into(),
            cached: Mutex::new(None),
        })
    }

    /// Uses a fresh password grant before expiry, avoiding cross-process
    /// refresh-token reuse.
    async fn token(&self, http: &Client) -> Result<SecretString> {
        let mut cached = self.cached.lock().await;
        if let Some((token, expires)) = cached.as_ref()
            && *expires > Instant::now()
        {
            return Ok(token.clone());
        }
        let response=http.post(format!("{}/token?grant_type=password",self.url)).header("apikey",self.api_key.expose_secret()).json(&json!({"email":self.email.expose_secret(),"password":self.password.expose_secret()})).send().await.map_err(|_|anyhow::anyhow!("Staging authentication transport failed"))?;
        ensure!(
            response.status().is_success(),
            "Staging authentication rejected with HTTP {}",
            response.status().as_u16()
        );
        #[derive(serde::Deserialize)]
        struct Session {
            access_token: String,
            expires_in: u64,
        }
        let session: Session = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("Invalid staging authentication response"))?;
        ensure!(session.expires_in > 120, "Staging session lifetime is too short");
        let token = SecretString::from(session.access_token);
        *cached =
            Some((token.clone(), Instant::now() + Duration::from_secs(session.expires_in - 60)));
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::platform::is_stopped;

    #[test]
    fn decodes_tagged_runtime_status() {
        assert!(is_stopped(&json!({"pipeline_id":1,"status":{"name":"stopped"}})));
        assert!(!is_stopped(&json!({"status":{"name":"stopping"}})));
        assert!(!is_stopped(&json!({"status":"stopped"})));
        assert!(!is_stopped(&serde_json::Value::Null));
    }
}
