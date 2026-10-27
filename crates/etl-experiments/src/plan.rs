//! Validated, credential-free experiment plans and rollout profiles.

use std::path::Path;

use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Successively larger runs of the same experiment.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Profile {
    /// Small end-to-end harness qualification.
    Smoke,
    /// Operator-calibrated workload.
    Baseline,
    /// Twice the baseline tables and total operations.
    Double,
}

/// How the ordinary hosted pipeline is selected.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Launch {
    /// Create the pipeline through Platform.
    #[default]
    Platform,
    /// Adopt a stopped pipeline created in Studio for this fixture.
    Attach {
        /// Existing stopped pipeline prepared for this fixture.
        pipeline_id: u64,
    },
}

/// Explicit intervention applied only after initial synchronization.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Retention {
    /// Ordinary committed transactions only.
    #[default]
    None,
    /// Keep one source write transaction open while other writers commit.
    HeldTransaction {
        /// Cluster WAL volume after which the held write commits.
        release_after_wal_bytes: u64,
    },
    /// Commit this many row updates in a single source transaction.
    LargeTransaction {
        /// Number of first-table rows updated before commit.
        rows: u64,
    },
}

/// One finite load stage; rates are aggregate targets across all writers.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Load {
    /// Human-readable stage name.
    pub(crate) name: String,
    /// Total committed row operations requested.
    pub(crate) operations: u64,
    /// Aggregate operation rate, without catch-up bursts after backpressure.
    pub(crate) operations_per_second: u32,
    /// Independent connections with disjoint primary-key ranges.
    pub(crate) writers: u16,
    /// Maximum operations in each transaction.
    pub(crate) transaction_rows: u32,
}

/// A group of tables sharing row size and replica identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableGroup {
    /// Number of tables in this group.
    pub(crate) tables: u16,
    /// Initial rows in each table.
    pub(crate) rows_per_table: u64,
    /// Text bytes in each initial row.
    pub(crate) payload_bytes: u32,
    /// Whether text is stored externally without TOAST compression.
    pub(crate) external_storage: bool,
    /// Whether updates carry full replica identity.
    pub(crate) replica_identity_full: bool,
}

/// Snowflake identifiers; authentication is supplied separately at runtime.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snowflake {
    /// Snowflake organization/account identifier.
    pub(crate) account: String,
    /// Dedicated Snowflake test user.
    pub(crate) user: String,
    /// Database in which the run owns one schema.
    pub(crate) database: String,
    /// Role used by ingestion and verification.
    pub(crate) role: String,
    /// Warehouse used only for SQL verification, not Snowpipe ingestion.
    pub(crate) warehouse: String,
}

/// Staging-only experiment input. Unknown fields fail instead of silently
/// changing the workload.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    /// Serialized plan contract version.
    pub(crate) format_version: u32,
    /// Unique identity shared by fixtures, jobs and artifacts.
    pub(crate) run_id: Uuid,
    /// Rollout level represented by this plan.
    pub(crate) profile: Profile,
    /// Managed staging project, also used as the pipeline tenant.
    pub(crate) project_ref: String,
    /// Exact direct database hostname checked against the credential URL.
    pub(crate) source_host: String,
    /// Destination and verification identifiers, without credentials.
    pub(crate) snowflake: Snowflake,
    /// Ordinary hosted pipeline creation or Studio attachment.
    #[serde(default)]
    pub(crate) launch: Launch,
    /// Optional registered replicator image ID; the test tenant must be
    /// allowlisted.
    pub(crate) version_id: Option<u64>,
    /// Pipeline options forwarded to Platform after injecting the publication.
    pub(crate) pipeline_config: serde_json::Value,
    /// Deterministic synthetic payload seed.
    pub(crate) seed: u64,
    /// Initial table-size and replica-identity distribution.
    pub(crate) groups: Vec<TableGroup>,
    /// Finite load concurrent with initial synchronization.
    pub(crate) copy_load: Load,
    /// Ordered CDC load stages after every table follows WAL.
    pub(crate) cdc_loads: Vec<Load>,
    /// Optional controlled intervention after initial synchronization.
    #[serde(default)]
    pub(crate) retention: Retention,
    /// Interval between observer samples.
    pub(crate) sample_seconds: u64,
    /// Maximum active experiment duration.
    pub(crate) deadline_seconds: u64,
    /// Maximum wait for destination convergence or restart-LSN release.
    pub(crate) convergence_seconds: u64,
    /// Per-slot retained WAL ceiling monitored by the observer.
    pub(crate) max_retained_wal_bytes: u64,
    /// Cluster-wide WAL ceiling after execution is claimed.
    pub(crate) max_generated_wal_bytes: u64,
    /// Per-process event journal byte ceiling.
    pub(crate) max_artifact_bytes: u64,
    /// Minimum fraction of the requested operation rate achieved by every
    /// stage.
    pub(crate) minimum_rate_ratio: f64,
}

impl Plan {
    /// Reads and validates a saved plan without connecting to any service.
    pub(crate) fn read(path: &Path) -> Result<Self> {
        let plan: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        plan.validate()?;
        Ok(plan)
    }

    /// Rejects unsafe identifiers, unbounded work, and inconsistent workload
    /// parameters.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.format_version == 1, "Unsupported plan version");
        identifier(&self.project_ref)?;
        ensure!(!self.source_host.is_empty(), "Source hostname is required");
        for value in [&self.snowflake.database, &self.snowflake.role, &self.snowflake.warehouse] {
            identifier(value)?;
        }
        ensure!(
            self.sample_seconds > 0 && self.sample_seconds <= 30,
            "Sample interval must be 1..30 seconds"
        );
        ensure!(
            self.deadline_seconds <= 604800
                && self.deadline_seconds > self.convergence_seconds
                && self.convergence_seconds > 0,
            "Invalid run deadlines"
        );
        ensure!(
            self.max_retained_wal_bytes > 0
                && self.max_generated_wal_bytes >= self.max_retained_wal_bytes,
            "Invalid WAL budgets"
        );
        ensure!(self.max_artifact_bytes >= 1_048_576, "Artifact budget must be at least 1 MiB");
        ensure!(
            self.minimum_rate_ratio > 0.0 && self.minimum_rate_ratio <= 1.0,
            "Invalid minimum rate ratio"
        );
        ensure!(!self.groups.is_empty() && self.table_count() <= 4096, "Expected 1..4096 tables");
        let max_writers =
            self.cdc_loads.iter().chain([&self.copy_load]).map(|l| l.writers).max().unwrap_or(0);
        for group in &self.groups {
            ensure!(
                group.tables > 0 && group.rows_per_table >= u64::from(max_writers) * 4,
                "Each table needs at least four rows per writer"
            );
            ensure!(
                (32..=1_048_576).contains(&group.payload_bytes),
                "Payload size must be 32 bytes..1 MiB"
            );
            ensure!(i64::try_from(group.rows_per_table).is_ok(), "Too many rows");
        }
        for load in self.cdc_loads.iter().chain([&self.copy_load]) {
            identifier(&load.name)?;
            ensure!(
                i64::try_from(load.operations).is_ok()
                    && load.operations > 0
                    && load.operations_per_second > 0
                    && load.writers > 0
                    && load.transaction_rows > 0,
                "Loads must be finite and nonzero"
            );
            ensure!(
                load.writers <= 128 && load.transaction_rows <= 100_000,
                "Load concurrency or transaction size exceeds bounds"
            );
        }
        ensure!(!self.cdc_loads.is_empty(), "At least one CDC stage is required");
        let names = self
            .cdc_loads
            .iter()
            .chain([&self.copy_load])
            .map(|load| &load.name)
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(names.len() == self.cdc_loads.len() + 1, "Load stage names must be unique");
        ensure!(
            self.pipeline_config.is_object()
                && self.pipeline_config.get("publication_name").is_none(),
            "Pipeline configuration must be an object without publication_name"
        );
        ensure!(
            self.pipeline_config.get("table_sync_copy").is_none(),
            "Experiments require initial copy of every table"
        );
        match self.retention {
            Retention::HeldTransaction { release_after_wal_bytes } => ensure!(
                release_after_wal_bytes > 0
                    && release_after_wal_bytes < self.max_retained_wal_bytes,
                "Held transaction must be released within the WAL budget"
            ),
            Retention::LargeTransaction { rows } => ensure!(
                rows > 0 && rows <= self.groups[0].rows_per_table,
                "Large transaction must fit the first table"
            ),
            Retention::None => {}
        }
        for key in
            self.pipeline_config.as_object().context("Invalid pipeline configuration")?.keys()
        {
            ensure!(
                [
                    "batch",
                    "table_error_retry_delay_ms",
                    "table_error_retry_max_attempts",
                    "max_table_sync_workers",
                    "max_copy_connections_per_table",
                    "memory_refresh_interval_ms",
                    "replication_lag_refresh_interval_ms",
                    "memory_backpressure",
                    "invalidated_slot_behavior",
                    "log_level"
                ]
                .contains(&key.as_str()),
                "Unsupported pipeline option: {key}"
            );
        }
        self.fixture_bytes()?;
        Ok(())
    }

    /// Namespace unique to this run on both databases.
    pub(crate) fn schema(&self) -> String {
        format!("exp_{}", self.run_id.simple())
    }

    /// Publication includes only the workload tables, never the control record.
    pub(crate) fn publication(&self) -> String {
        format!("{}_pub", self.schema())
    }

    /// Total workload tables, excluding the control record.
    pub(crate) fn table_count(&self) -> usize {
        self.groups.iter().map(|g| usize::from(g.tables)).sum()
    }

    /// Logical initial text volume; physical source storage and WAL are
    /// measured separately.
    pub(crate) fn fixture_bytes(&self) -> Result<u64> {
        self.groups.iter().try_fold(0_u64, |total, group| {
            u64::from(group.tables)
                .checked_mul(group.rows_per_table)
                .and_then(|n| n.checked_mul(u64::from(group.payload_bytes)))
                .and_then(|n| total.checked_add(n))
                .context("Fixture byte count overflow")
        })
    }

    /// Stable fixture identity permits adding a Studio pipeline ID after
    /// preparation.
    pub(crate) fn fixture_hash(&self) -> Result<String> {
        Ok(Sha256::digest(serde_json::to_vec(&(
            self.run_id,
            &self.project_ref,
            &self.source_host,
            self.seed,
            &self.groups,
            &self.snowflake,
        ))?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
    }

    /// Derives a fresh rollout run without changing the workload distribution.
    pub(crate) fn derive(&self, profile: Profile) -> Result<Self> {
        let mut plan = self.clone();
        plan.run_id = Uuid::new_v4();
        plan.profile = profile;
        plan.launch = Launch::Platform;
        match profile {
            Profile::Smoke => {
                plan.groups = vec![TableGroup {
                    tables: 8,
                    rows_per_table: 128,
                    payload_bytes: 16_384,
                    external_storage: true,
                    replica_identity_full: false,
                }];
                plan.copy_load = Load {
                    name: "during_copy".into(),
                    operations: 256,
                    operations_per_second: 32,
                    writers: 2,
                    transaction_rows: 8,
                };
                plan.cdc_loads = vec![Load {
                    name: "steady".into(),
                    operations: 512,
                    operations_per_second: 32,
                    writers: 2,
                    transaction_rows: 8,
                }];
                plan.retention = Retention::None;
                plan.deadline_seconds = 3600;
                plan.convergence_seconds = 600;
                plan.max_artifact_bytes = 67_108_864;
                plan.max_retained_wal_bytes = 134_217_728;
                plan.max_generated_wal_bytes = 2_147_483_648;
            }
            Profile::Baseline => {}
            Profile::Double => {
                ensure!(
                    matches!(self.profile, Profile::Baseline),
                    "Double must be derived from a baseline plan"
                );
                for group in &mut plan.groups {
                    group.tables = group.tables.checked_mul(2).context("Table count overflow")?;
                }
                for load in plan.cdc_loads.iter_mut().chain([&mut plan.copy_load]) {
                    load.operations =
                        load.operations.checked_mul(2).context("Operation count overflow")?;
                }
            }
        }
        plan.validate()?;
        Ok(plan)
    }
}

/// Restricts generated SQL identifiers to a portable quoted subset.
pub(crate) fn identifier(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 63
            && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "Invalid identifier"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::plan::{Plan, Profile};

    #[test]
    fn double_preserves_distribution_and_doubles_volume() {
        let baseline: Plan = serde_json::from_str(include_str!("../specs/baseline.json")).unwrap();
        let double = baseline.derive(Profile::Double).unwrap();
        assert_eq!(double.table_count(), 2 * baseline.table_count());
        assert_eq!(double.fixture_bytes().unwrap(), 2 * baseline.fixture_bytes().unwrap());
        assert_eq!(double.cdc_loads[0].operations, 2 * baseline.cdc_loads[0].operations);
        assert_eq!(
            double.cdc_loads[0].operations_per_second,
            baseline.cdc_loads[0].operations_per_second
        );
        assert_ne!(double.run_id, baseline.run_id);
    }

    #[test]
    fn rejects_unbounded_or_ambiguous_plans() {
        let mut plan: Plan = serde_json::from_str(include_str!("../specs/baseline.json")).unwrap();
        plan.sample_seconds = 0;
        assert!(plan.validate().is_err());
        plan.sample_seconds = 5;
        plan.pipeline_config["publication_name"] = "unrelated".into();
        assert!(plan.validate().is_err());
    }
}
