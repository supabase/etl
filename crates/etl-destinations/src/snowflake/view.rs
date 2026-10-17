//! Lifecycle of the ordinary view over an ETL-owned Snowflake CDC table.
//!
//! The public name is unavailable during incompatible schema changes and
//! resync. Renaming the view aside preserves grants until its definition can be
//! replaced against an applied schema. Remote names and ownership comments make
//! retries work without adding a second durable schema state machine.
//! Eligibility loss is rejected before advancing the applied raw schema, so an
//! owned view remains proof of eligibility during startup reconciliation.

use etl::{
    pipeline::PipelineId,
    schema::{ReplicatedTableSchema, TableId},
};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::snowflake::{
    Error, Result,
    auth::TokenProvider,
    materialization::{validate_schema, view_query},
    sql::{quote_identifier, quote_string_literal},
    sql_client::SqlClient,
};

/// Fingerprints a definition or a long raw name using the same stable encoding.
fn fingerprint(value: &str) -> String {
    Sha256::digest(value.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Ordinary current-state view and its reserved name during schema transitions.
pub(super) struct CompanionView {
    /// Customer-facing view name in the raw table's namespace.
    name: String,
    /// Reserved name that retains ownership and grants while the view is
    /// offline.
    pending: String,
    /// Exact owner prefix; the remaining comment is the definition fingerprint.
    owner: String,
}

impl CompanionView {
    /// Derives companion names from the raw name resolved by the caller from
    /// the owned source snapshot.
    pub(super) fn new(pipeline: PipelineId, table_id: TableId, table: &str) -> Self {
        // Snowflake identifiers allow 255 characters. Reserve room for the
        // temporary suffix too; the fallback prefix cannot be a raw table name.
        let name = if table.chars().count() + "_CURRENT_PENDING".len() > 255 {
            format!("_ETL_{}_CURRENT", fingerprint(table).to_ascii_uppercase())
        } else {
            format!("{table}_CURRENT")
        };
        Self {
            pending: format!("{name}_PENDING"),
            name,
            owner: format!(
                "supabase-etl:view:v1:{}:",
                serde_json::json!([pipeline, table_id.into_inner(), table])
            ),
        }
    }

    /// Rejects loss of materialization eligibility before the caller changes
    /// the applied raw schema, preserving owned view grants.
    ///
    /// Supported schemas require no remote check. Unsupported schemas may
    /// continue raw-only ingestion when no owned companion view exists.
    pub(super) async fn check_schema<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        schema: &ReplicatedTableSchema,
    ) -> Result<bool> {
        match validate_schema(schema) {
            Ok(()) => Ok(true),
            Err(error @ (Error::UnsupportedIdentity(_) | Error::Config(_))) => {
                if self.existing(sql, false).await?.is_some() {
                    self.park(sql).await?;
                    return Err(Error::CompanionViewUnsupported {
                        name: self.name.clone(),
                        pending: self.pending.clone(),
                        source: Box::new(error),
                    });
                }
                warn!(
                    table_name = %schema.name(),
                    "snowflake table has no companion view: materialization requires a supported \
                     primary-key identity and source names outside the _cdc_ prefix"
                );
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Reconciles a previously owned view using the applied raw schema.
    ///
    /// Ownership proves that a source schema previously established view
    /// eligibility. Without that proof, startup must wait for actual source
    /// identity metadata instead of inferring it from primary-key columns.
    pub(super) async fn restore<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        table: &str,
        schema: &ReplicatedTableSchema,
    ) -> Result<bool> {
        if self.existing(sql, false).await?.is_none() {
            return Ok(false);
        }
        self.prepare(sql, table, schema).await?;
        Ok(true)
    }

    /// Prepares a view after the caller has established a usable raw schema.
    ///
    /// Normal setup supplies actual source identity. Startup restoration may
    /// instead rely on an existing owned view as proof of eligibility.
    ///
    /// Initial creation is retriable under Creating; later replacements require
    /// Applied metadata. A failed replacement leaves the parked view and grants
    /// available for the next attempt. Unsupported complete-row schemas retain
    /// their existing raw-only behavior.
    pub(super) async fn prepare<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        table: &str,
        schema: &ReplicatedTableSchema,
    ) -> Result<()> {
        if !self.check_schema(sql, schema).await? {
            return Ok(());
        }
        let query = view_query(table, schema)?;
        let mut columns = schema
            .column_schemas()
            .map(|column| {
                let metadata = serde_json::json!({
                    "column_id": column.ordinal_position,
                    "primary_key_position": column.primary_key_ordinal_position,
                })
                .to_string();
                format!(
                    "{} comment {}",
                    quote_identifier(&column.name),
                    quote_string_literal(&metadata)
                )
            })
            .collect::<Vec<_>>();
        columns.extend([
            "\"_cdc_anchored\" comment 'False means the retained lifetime has no insert or \
             snapshot anchor'"
                .to_owned(),
            "\"_cdc_unresolved_columns\" comment 'Source column names with no supplied value in \
             the retained lifetime'"
                .to_owned(),
            "\"_cdc_last_sequence\" comment 'Latest event sequence for this current row'"
                .to_owned(),
        ]);
        let definition = format!("({}) as {query}", columns.join(", "));
        let marker = format!("{}{}", self.owner, fingerprint(&definition));
        let existing = self.existing(sql, true).await?;
        match existing {
            Some((name, comment)) if name == self.name && comment == marker => Ok(()),
            Some((name, _)) => {
                self.create(sql, &name, &columns, &query, &marker, true).await?;
                if name == self.pending {
                    self.rename(sql, &self.pending, &self.name).await?;
                }
                Ok(())
            }
            None => self.create(sql, &self.name, &columns, &query, &marker, false).await,
        }
    }

    /// Removes the public name before any raw-table DDL, retaining access
    /// grants.
    pub(super) async fn park<T: TokenProvider>(&self, sql: &SqlClient<T>) -> Result<()> {
        if let Some((name, _)) = self.existing(sql, false).await?
            && name == self.name
        {
            self.rename(sql, &self.name, &self.pending).await?;
        }
        Ok(())
    }

    /// Removes a view when resync moves this source to another physical name.
    pub(super) async fn remove<T: TokenProvider>(&self, sql: &SqlClient<T>) -> Result<()> {
        if let Some((name, _)) = self.existing(sql, false).await? {
            sql.execute_ddl(&format!("drop view {}", sql.fully_qualified_name(&name))).await?;
        }
        Ok(())
    }

    /// Returns the sole owned view. Creation requires both names to be free of
    /// foreign objects; raw-only tables and cleanup must leave them untouched.
    async fn existing<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        reject_foreign: bool,
    ) -> Result<Option<(String, String)>> {
        let objects = sql.objects_starting_with(&self.name).await?;
        let mut existing = None;
        for (name, kind, comment) in objects {
            if name != self.name && name != self.pending {
                continue;
            }
            let signature = comment.strip_prefix(&self.owner);
            if kind != "VIEW"
                || !signature.is_some_and(|value| {
                    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
                })
            {
                if reject_foreign {
                    return Err(Error::Config(format!(
                        "Snowflake object '{name}' conflicts with an ETL companion view; use an \
                         unused destination namespace or resolve the collision"
                    )));
                }
                continue;
            }
            if existing.is_some() {
                return Err(Error::Config(format!(
                    "Snowflake companion view '{}' has both public and pending objects; resolve \
                     the ambiguous state before retrying",
                    self.name
                )));
            }
            existing = Some((name, comment));
        }
        Ok(existing)
    }

    /// Publishes ownership, mapping, and SQL together; replacement preserves
    /// grants.
    async fn create<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        name: &str,
        columns: &[String],
        query: &str,
        marker: &str,
        replace: bool,
    ) -> Result<()> {
        // A plain CREATE fails safely if an unseen or concurrently created
        // object occupies the name. Only a verified owned object is replaced.
        let replace_clause = if replace { "or replace " } else { "" };
        let grants = if replace { "copy grants " } else { "" };
        sql.execute_ddl(&format!(
            "create {replace_clause}view {} ({}) {grants}comment = {} as {query}",
            sql.fully_qualified_name(name),
            columns.join(", "),
            quote_string_literal(marker),
        ))
        .await
    }

    /// Renames within the same schema without replacing an occupied target
    /// name.
    async fn rename<T: TokenProvider>(
        &self,
        sql: &SqlClient<T>,
        from: &str,
        to: &str,
    ) -> Result<()> {
        sql.execute_ddl(&format!(
            "alter view {} rename to {}",
            sql.fully_qualified_name(from),
            sql.fully_qualified_name(to)
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type};

    use crate::{
        snowflake::{Config, Error, SqlClient, auth::TokenProvider, view::CompanionView},
        table_name::try_stringify_table_name,
    };

    /// Rejects unexpected remote work during schema preflight.
    struct UnusedTokenProvider;

    impl TokenProvider for UnusedTokenProvider {
        async fn get_token(&self) -> crate::snowflake::Result<String> {
            Err(Error::Auth("Remote SQL is not expected".to_owned()))
        }

        async fn invalidate_token(&self) {}
    }

    /// Ordinary supported relations do not need a remote ownership lookup.
    #[tokio::test]
    async fn supported_schema_preflight_requires_no_remote_work() {
        let schema = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".into(), "users".into()),
            vec![ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1)],
        )));
        let sql = SqlClient::new(
            Config::new("example-account", "test-user", "test-db", "test-schema").unwrap(),
            Arc::new(UnusedTokenProvider),
            reqwest::Client::new(),
        );

        assert!(
            CompanionView::new(1, schema.id(), "PUBLIC_USERS")
                .check_schema(&sql, &schema)
                .await
                .unwrap()
        );
    }

    #[test]
    fn companion_names_do_not_collide_with_encoded_source_names() {
        let long = format!("a{}b", "_".repeat(61));
        let parts = [
            "public",
            "users",
            "users_current",
            "users_current_pending",
            "_users",
            "users_",
            "_",
            "A_B",
            "current",
            "schéma",
            "用户",
            &long,
        ];
        let raw = parts
            .iter()
            .flat_map(|schema| {
                parts.iter().map(move |table| {
                    try_stringify_table_name(&TableName::new((*schema).into(), (*table).into()))
                        .ok()
                        .map(|name| name.to_uppercase())
                })
            })
            .flatten()
            .collect::<std::collections::HashSet<_>>();
        for length in [239, 240] {
            let name = "é".repeat(length);
            let view = CompanionView::new(1, TableId::new(1), &name);
            assert_eq!(view.name.starts_with("_ETL_"), length == 240);
            assert!(view.pending.chars().count() <= 255);
        }
        let mut views = std::collections::HashSet::new();
        for name in &raw {
            let view = CompanionView::new(1, TableId::new(1), name);
            assert!(view.pending.chars().count() <= 255);
            assert!(!raw.contains(&view.name));
            assert!(!raw.contains(&view.pending));
            assert!(views.insert(view.name));
            assert!(views.insert(view.pending));
        }
    }
}
