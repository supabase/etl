//! Bounded SQL verification through the real Snowflake SQL API.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use etl_destinations::snowflake::{AuthManager, Config, HttpExchanger, TokenProvider};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::plan::Plan;

/// SQL reader sharing the destination's key-pair authentication implementation.
pub(crate) struct Snowflake {
    http: Client,
    auth: AuthManager<HttpExchanger>,
    base: String,
    plan: Plan,
}

/// Loads the mounted private key without putting it in plans, arguments, or
/// artifacts.
pub(crate) fn private_key() -> Result<SecretString> {
    let path = std::env::var("EXPERIMENT_SNOWFLAKE_PRIVATE_KEY_FILE")
        .context("EXPERIMENT_SNOWFLAKE_PRIVATE_KEY_FILE is required")?;
    Ok(std::fs::read_to_string(path).context("Cannot read Snowflake private key file")?.into())
}

impl Snowflake {
    /// Builds the SQL API client using the test destination's key-pair
    /// identity.
    pub(crate) fn new(plan: &Plan) -> Result<Self> {
        let c = &plan.snowflake;
        let config = Config::new(&c.account, &c.user, &c.database, &plan.schema())
            .map_err(|_| anyhow::anyhow!("Invalid Snowflake configuration"))?
            .with_private_key(
                private_key()?,
                std::env::var("EXPERIMENT_SNOWFLAKE_KEY_PASSPHRASE").ok().map(SecretString::from),
            );
        let base = config.account_url().to_owned();
        let auth = AuthManager::new(config)
            .map_err(|_| anyhow::anyhow!("Snowflake authentication setup failed"))?;
        Ok(Self {
            http: Client::builder()
                .timeout(Duration::from_secs(90))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            auth,
            base,
            plan: plan.clone(),
        })
    }

    /// Executes one bounded statement, retaining the request ID across
    /// authentication refresh.
    pub(crate) async fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let request_id = Uuid::new_v4();
        let mut handle: Option<String> = None;
        let mut refreshed = false;
        loop {
            ensure!(tokio::time::Instant::now() < deadline, "Snowflake SQL deadline exceeded");
            let token = SecretString::from(
                self.auth
                    .get_token()
                    .await
                    .map_err(|_| anyhow::anyhow!("Snowflake authentication failed"))?,
            );
            let request = if let Some(handle) = &handle {
                self.http.get(format!("{}/api/v2/statements/{handle}", self.base))
            } else {
                self.http.post(format!("{}/api/v2/statements",self.base)).query(&[("requestId",request_id.to_string())])
                    .json(&json!({"statement":sql,"database":self.plan.snowflake.database,"schema":"PUBLIC","role":self.plan.snowflake.role,"warehouse":self.plan.snowflake.warehouse,"timeout":120,"parameters":{"QUOTED_IDENTIFIERS_IGNORE_CASE":"false"}}))
            };
            let response = request
                .bearer_auth(token.expose_secret())
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("Snowflake SQL transport failed"))?;
            let status = response.status();
            if status == StatusCode::UNAUTHORIZED && !refreshed {
                self.auth.invalidate_token().await;
                refreshed = true;
                continue;
            }
            ensure!(status.is_success(), "Snowflake SQL failed with HTTP {}", status.as_u16());
            let value: Value = response
                .json()
                .await
                .map_err(|_| anyhow::anyhow!("Invalid Snowflake SQL response"))?;
            if status == StatusCode::ACCEPTED {
                let next = value["statementHandle"]
                    .as_str()
                    .context("Missing Snowflake statement handle")?;
                Uuid::parse_str(next).context("Invalid Snowflake statement handle")?;
                handle = Some(next.to_owned());
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            return complete_rows(value);
        }
    }

    /// Creates a unique schema. Existing schemas are never silently adopted.
    pub(crate) async fn prepare(&self) -> Result<()> {
        self.query(&format!(
            "create schema \"{}\".\"{}\"",
            self.plan.snowflake.database,
            self.plan.schema()
        ))
        .await?;
        Ok(())
    }

    /// Checks existence after an ambiguous schema-creation outcome without
    /// treating existence as proof of ownership.
    pub(crate) async fn schema_exists(&self) -> Result<bool> {
        let rows = self
            .query(&format!(
                "select count(*)::varchar from \"{}\".information_schema.schemata where \
                 schema_name='{}'",
                self.plan.snowflake.database,
                self.plan.schema()
            ))
            .await?;
        Ok(rows.first().and_then(|row| row.first()).and_then(Value::as_str) == Some("1"))
    }

    /// Requests the destination configuration only at the authenticated
    /// Platform boundary.
    pub(crate) fn destination_body(&self) -> Result<Value> {
        let key = private_key()?;
        let passphrase =
            std::env::var("EXPERIMENT_SNOWFLAKE_KEY_PASSPHRASE").ok().map(SecretString::from);
        Ok(
            json!({"name":self.plan.schema(),"config":{"snowflake":{"account_id":self.plan.snowflake.account,"user":self.plan.snowflake.user,"database":self.plan.snowflake.database,"schema":self.plan.schema(),"role":self.plan.snowflake.role,"private_key":key.expose_secret(),"private_key_passphrase":passphrase.as_ref().map(ExposeSecret::expose_secret)}}}),
        )
    }

    /// Companion view name follows the destination's legacy naming for
    /// generated identifiers.
    fn view(&self, table: &str) -> String {
        view_name(&self.plan, table)
    }

    /// Observes the final committed marker in the destination companion view.
    pub(crate) async fn marker(&self, table: &str) -> Result<bool> {
        let rows = self
            .query(&format!(
                "select count(*)::varchar from {} where \"id\"=0 and \"revision\"=-1",
                self.view(table)
            ))
            .await?;
        Ok(rows.first().and_then(|r| r.first()).and_then(Value::as_str) == Some("1"))
    }

    /// Reads complete pages with explicit SQL NULL and JSON null preservation.
    pub(crate) async fn rows(&self, table: &str, after: i64, limit: i64) -> Result<Vec<Value>> {
        let rows = self
            .query(&format!(
                "select \"id\"::varchar,\"revision\"::varchar,\"payload\",to_json(\"metadata\"), \
                 (\"metadata\" is null)::varchar from {} where \"id\">{after} order by \"id\" \
                 limit {limit}",
                self.view(table)
            ))
            .await?;
        rows.into_iter().map(normalize_row).collect()
    }
}

/// Uses the destination's legacy underscore encoding for our generated ASCII
/// names, which never begin or end with underscores or exceed Snowflake limits.
fn view_name(plan: &Plan, table: &str) -> String {
    format!(
        "\"{}\".\"{}\".\"{}_{}_CURRENT\"",
        plan.snowflake.database,
        plan.schema(),
        plan.schema().replace('_', "__").to_uppercase(),
        table.replace('_', "__").to_uppercase()
    )
}

/// Preserves JSON null separately from SQL NULL when decoding SQL API rows.
fn normalize_row(mut row: Vec<Value>) -> Result<Value> {
    ensure!(row.len() == 5, "Invalid Snowflake verification row width");
    if let Some(text) = row[3].as_str() {
        row[3] = serde_json::from_str(text).context("Invalid JSON in verification row")?;
    }
    row[4] = match row[4].as_str() {
        Some("true" | "TRUE") => true.into(),
        Some("false" | "FALSE") => false.into(),
        _ => anyhow::bail!("Invalid Snowflake null indicator"),
    };
    Ok(Value::Array(row))
}

/// Fails closed if Snowflake partitions or truncates a result instead of
/// treating a prefix as complete.
fn complete_rows(value: Value) -> Result<Vec<Vec<Value>>> {
    let total = value["resultSetMetaData"]["numRows"]
        .as_u64()
        .context("Snowflake result lacks total row count")?;
    let rows: Vec<Vec<Value>> =
        serde_json::from_value(value["data"].clone()).context("Snowflake result lacks row data")?;
    ensure!(
        total == u64::try_from(rows.len())?,
        "Incomplete Snowflake result; reduce verification page size"
    );
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::snowflake::{complete_rows, normalize_row, view_name};

    #[test]
    fn generated_view_names_follow_destination_underscore_encoding() {
        let mut plan: crate::plan::Plan =
            serde_json::from_str(include_str!("../specs/baseline.json")).unwrap();
        plan.run_id = uuid::Uuid::nil();
        plan.snowflake.database = "TEST".into();
        assert_eq!(
            view_name(&plan, "t0000"),
            "\"TEST\".\"exp_00000000000000000000000000000000\".\"\
             EXP__00000000000000000000000000000000_T0000_CURRENT\""
        );
    }

    #[test]
    fn distinguishes_json_null_from_sql_null_and_rejects_bad_rows() {
        let decode = |value: serde_json::Value| normalize_row(value.as_array().unwrap().clone());
        assert_eq!(
            decode(json!(["1", "2", null, "null", "false"])).unwrap(),
            json!(["1", "2", null, null, false])
        );
        assert_eq!(
            decode(json!(["1", "2", null, null, "true"])).unwrap(),
            json!(["1", "2", null, null, true])
        );
        assert!(decode(json!(["1", "2", null, null])).is_err());
        assert!(decode(json!(["1", "2", null, "invalid", "false"])).is_err());
    }

    #[test]
    fn rejects_partial_provider_results() {
        assert!(complete_rows(json!({"resultSetMetaData":{"numRows":2},"data":[["1"]]})).is_err());
        assert!(complete_rows(json!({"data":[]})).is_err());
        assert_eq!(
            complete_rows(json!({"resultSetMetaData":{"numRows":1},"data":[[null]]})).unwrap(),
            vec![vec![serde_json::Value::Null]]
        );
    }
}
