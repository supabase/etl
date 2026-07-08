use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use pg_escape::{quote_identifier, quote_literal};
use serde_json::{Map, Number, Value, json};

const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_PORT: u16 = 5430;
const DEFAULT_DB_NAME: &str = "bench";
const DEFAULT_DB_USER: &str = "postgres";
const DEFAULT_DB_PASSWORD: &str = "postgres";
const DEFAULT_TPCC_TABLES: &str =
    "customer,district,item,new_order,order_line,orders,stock,warehouse";
const DEFAULT_TPCC_WAREHOUSES: u16 = 4;
const DEFAULT_STREAMING_DURATION_SECONDS: u64 = 60;
const DEFAULT_STREAMING_DRAIN_QUIET_MS: u64 = 2_000;
const DEFAULT_BATCH_MAX_FILL_MS: u64 = 1_000;
const DEFAULT_NULL_FLUSH_DELAY_MIN_MS: u64 = 10;
const DEFAULT_NULL_FLUSH_DELAY_MAX_MS: u64 = 100;
const DEFAULT_MAX_TABLE_SYNC_WORKERS: u16 = 4;
const DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE: u16 = 4;
const DEFAULT_MEMORY_BUDGET_RATIO: f32 = 0.2;
const DEFAULT_TPCC_THREADS_PER_WAREHOUSE: usize = 8;
const DEFAULT_TPCC_THREADS_PER_CPU: usize = 2;
const DEFAULT_TPCC_MIN_THREADS: usize = 8;
const DEFAULT_TPCC_MAX_THREADS: usize = 64;
const MEMORY_BACKPRESSURE_ACTIVATE_THRESHOLD: f32 = 0.85;
const MEMORY_BACKPRESSURE_RESUME_THRESHOLD: f32 = 0.75;
const BENCH_SNOWFLAKE_CONNECTION_ENV: &str = "BENCH_SNOWFLAKE_CONNECTION";
const DEFAULT_HOTPATH_MCP_PORT: u16 = 6771;
const CPU_SAMPLE_SUMMARY_FILTER_PATTERNS: &[&str] = &[
    "hotpath::",
    "hotpath..",
    "addr2line",
    "aho_corasick",
    "clap::",
    "clap_",
    "clap-builder",
    "clap_builder",
    "crossbeam_channel",
    "gimli::",
    "object::",
    "rustc_demangle",
];
const CPU_SAMPLE_SUMMARY_ETL_PATTERNS: &[&str] =
    &["etl::", "etl_benchmarks::", "table_copy::", "table_streaming::"];

#[derive(Args)]
pub(crate) struct BenchmarkArgs {
    /// Postgres host.
    #[arg(long, env = "POSTGRES_HOST", default_value = DEFAULT_DB_HOST)]
    host: String,
    /// Postgres port.
    #[arg(long, env = "POSTGRES_PORT", default_value_t = DEFAULT_DB_PORT)]
    port: u16,
    /// Database name.
    #[arg(long, env = "POSTGRES_DB", default_value = DEFAULT_DB_NAME)]
    database: String,
    /// Postgres username.
    #[arg(long, env = "POSTGRES_USER", default_value = DEFAULT_DB_USER)]
    username: String,
    /// Postgres password.
    #[arg(long, env = "POSTGRES_PASSWORD", default_value = DEFAULT_DB_PASSWORD)]
    password: String,
    /// Number of TPC-C warehouses to prepare.
    #[arg(long, env = "TPCC_WAREHOUSES", default_value_t = DEFAULT_TPCC_WAREHOUSES)]
    warehouses: u16,
    /// Threads to use when preparing TPC-C data. Defaults to a warehouse and
    /// CPU based value.
    #[arg(long, env = "TPCC_THREADS")]
    tpcc_threads: Option<u16>,
    /// Comma-separated TPC-C tables to copy.
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_TPCC_TABLES)]
    tpcc_tables: Vec<String>,
    /// Drop and regenerate existing TPC-C tables.
    #[arg(long, default_value_t = false)]
    force_prepare: bool,
    /// Skip TPC-C preparation.
    #[arg(long, default_value_t = false)]
    skip_prepare: bool,
    /// Skip the table-copy benchmark.
    #[arg(long, default_value_t = false)]
    skip_table_copy: bool,
    /// Skip the table-streaming benchmark.
    #[arg(long, default_value_t = false)]
    skip_table_streaming: bool,
    /// Destination to benchmark.
    #[arg(long, value_enum, default_value = "null")]
    destination: Destination,
    /// BigQuery project ID.
    #[arg(long, env = "BQ_PROJECT_ID")]
    bq_project_id: Option<String>,
    /// BigQuery dataset ID.
    #[arg(long, env = "BQ_DATASET_ID")]
    bq_dataset_id: Option<String>,
    /// BigQuery service account key file.
    #[arg(long, env = "BQ_SA_KEY_FILE")]
    bq_sa_key_file: Option<PathBuf>,
    /// ClickHouse HTTP URL (for example, `http://localhost:8123`).
    #[arg(long, env = "BENCH_CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,
    /// ClickHouse username.
    #[arg(long, env = "BENCH_CLICKHOUSE_USER")]
    clickhouse_user: Option<String>,
    /// ClickHouse password.
    #[arg(long, env = "BENCH_CLICKHOUSE_PASSWORD")]
    clickhouse_password: Option<String>,
    /// ClickHouse database. Must already exist.
    #[arg(long, env = "BENCH_CLICKHOUSE_DATABASE")]
    clickhouse_database: Option<String>,
    /// Run the TPC-C streaming workload for this many seconds.
    #[arg(long)]
    streaming_duration_seconds: Option<u64>,
    /// Quiet period with no CDC events before TPC-C streaming is considered
    /// drained.
    #[arg(long, default_value_t = DEFAULT_STREAMING_DRAIN_QUIET_MS)]
    streaming_drain_quiet_ms: u64,
    /// Maximum batch fill time in milliseconds.
    #[arg(long, default_value_t = DEFAULT_BATCH_MAX_FILL_MS)]
    batch_max_fill_ms: u64,
    /// Minimum artificial null-destination flush delay in milliseconds.
    #[arg(long, default_value_t = DEFAULT_NULL_FLUSH_DELAY_MIN_MS)]
    null_flush_delay_min_ms: u64,
    /// Maximum artificial null-destination flush delay in milliseconds.
    #[arg(long, default_value_t = DEFAULT_NULL_FLUSH_DELAY_MAX_MS)]
    null_flush_delay_max_ms: u64,
    /// Ratio of process memory reserved for stream batch bytes.
    #[arg(long, default_value_t = DEFAULT_MEMORY_BUDGET_RATIO)]
    memory_budget_ratio: f32,
    /// Maximum table sync workers.
    #[arg(long, default_value_t = DEFAULT_MAX_TABLE_SYNC_WORKERS)]
    max_table_sync_workers: u16,
    /// Maximum worker connections per table during initial copy.
    #[arg(long, default_value_t = DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE)]
    max_copy_connections_per_table: u16,
    /// Enable ETL memory backpressure. Benchmark orchestration disables it by
    /// default.
    #[arg(long, default_value_t = false)]
    enable_memory_backpressure: bool,
    /// Number of measured samples to run for each selected benchmark.
    #[arg(long, default_value_t = 1)]
    samples: u16,
    /// Number of warmup samples to run before measured samples. Warmups are
    /// discarded from the aggregate report.
    #[arg(long, default_value_t = 0)]
    warmup_samples: u16,
    /// Directory where JSON benchmark reports are written.
    #[arg(long, default_value = "target/bench-results")]
    output_dir: PathBuf,
    /// Cargo profile used for benchmark binaries.
    #[arg(long, default_value = "release")]
    benchmark_profile: String,
    /// Enable HotPath timing and Tokio runtime profiling.
    #[arg(long, default_value_t = false)]
    hotpath: bool,
    /// Enable HotPath allocation profiling.
    #[arg(long, default_value_t = false)]
    hotpath_alloc: bool,
    /// Enable HotPath CPU sampling.
    #[arg(long, default_value_t = false)]
    hotpath_cpu: bool,
    /// Comma-separated benchmarks that should expose the HotPath MCP server.
    ///
    /// Supported values are `table_copy`, `table_streaming`, and `all`.
    #[arg(long, value_delimiter = ',')]
    hotpath_mcp_benchmarks: Vec<String>,
    /// Port used by the unauthenticated HotPath MCP server.
    #[arg(long, default_value_t = DEFAULT_HOTPATH_MCP_PORT)]
    hotpath_mcp_port: u16,
    /// Directory where per-benchmark HotPath JSON reports are written.
    ///
    /// Defaults to `<output-dir>/hotpath` when any HotPath mode is enabled.
    #[arg(long)]
    hotpath_output_dir: Option<PathBuf>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Destination {
    Null,
    #[value(name = "bigquery")]
    BigQuery,
    #[value(name = "clickhouse")]
    ClickHouse,
    #[value(name = "snowflake")]
    Snowflake,
}

impl BenchmarkArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.validate()?;
        check_command("psql")?;
        let runs_tpcc_streaming = self.runs_tpcc_streaming();
        let needs_tpcc_tables = self.needs_tpcc_tables();
        if (!self.skip_prepare && needs_tpcc_tables) || runs_tpcc_streaming {
            check_command("go-tpc")?;
        }

        fs::create_dir_all(&self.output_dir).context("failed to create benchmark output dir")?;
        if let Some(hotpath_output_dir) = self.effective_hotpath_output_dir() {
            fs::create_dir_all(&hotpath_output_dir)
                .context("failed to create HotPath output dir")?;
        }

        let tpcc_threads =
            self.tpcc_threads.unwrap_or_else(|| recommended_threads(self.warehouses));
        let pipeline_id_base = benchmark_pipeline_id_base()?;

        print_configuration(&[
            ("Database", format!("{}@{}:{}", self.database, self.host, self.port)),
            ("Warehouses", self.warehouses.to_string()),
            ("TPC-C threads", tpcc_threads.to_string()),
            ("TPC-C tables", self.tpcc_tables.join(",")),
            ("Streaming workload", "tpcc".to_owned()),
            (
                "Streaming duration",
                format!(
                    "{} seconds",
                    self.streaming_duration_seconds.unwrap_or(DEFAULT_STREAMING_DURATION_SECONDS)
                ),
            ),
            ("Streaming drain quiet", format!("{} ms", self.streaming_drain_quiet_ms)),
            ("Batch max fill", format!("{} ms", self.batch_max_fill_ms)),
            ("Null flush delay", self.null_flush_delay_label()),
            ("Table-sync workers", self.max_table_sync_workers.to_string()),
            ("Copy connections/table", self.max_copy_connections_per_table.to_string()),
            ("Measured samples", self.samples.to_string()),
            ("Warmup samples", self.warmup_samples.to_string()),
            ("Pipeline id base", pipeline_id_base.to_string()),
            ("Destination", self.destination.as_arg().to_owned()),
            ("Memory backpressure", memory_backpressure_label(self.enable_memory_backpressure)),
            ("Memory budget ratio", format_decimal(f64::from(self.memory_budget_ratio), 2)),
            ("Output dir", self.output_dir.display().to_string()),
            ("Cargo profile", self.benchmark_profile.clone()),
            ("HotPath", self.hotpath_label()),
        ]);

        print_phase("Benchmark database", "ensuring it exists");
        self.ensure_database()?;
        print_done("Benchmark database", "ready");

        if needs_tpcc_tables && !self.skip_prepare {
            print_phase("TPC-C data", "preparing or reusing tables");
            self.prepare_tpcc(tpcc_threads)?;
            print_done("TPC-C data", "ready");
        } else if needs_tpcc_tables {
            print_phase("TPC-C data", "checking existing tables");
            self.ensure_tpcc_tables_exist()?;
            print_done("TPC-C data", "existing tables are ready");
        } else {
            print_skip("TPC-C data", "selected benchmarks do not need it");
        }

        let mut table_copy_report = None;
        let mut table_streaming_report = None;

        if !self.skip_table_copy {
            print_phase("Table copy", "preparing publication and row counts");
            let selected_tables = self.validated_tpcc_tables()?;
            let table_ids = self.fetch_table_ids(&selected_tables)?;
            let expected_row_count = self.fetch_expected_row_count(&selected_tables)?;
            print_phase(
                "Table copy",
                &format!(
                    "copying {} rows from {} TPC-C tables",
                    format_integer(expected_row_count),
                    selected_tables.len()
                ),
            );

            table_copy_report = Some(self.run_table_copy_samples(
                &selected_tables,
                &table_ids,
                expected_row_count,
                pipeline_id_base,
                self.output_dir.join("table_copy.json"),
            )?);
            print_done("Table copy", "finished");
        } else {
            print_skip("Table copy", "disabled by flag");
        }

        if !self.skip_table_streaming {
            print_phase("Table streaming", "preparing benchmark");
            table_streaming_report = Some(self.run_table_streaming_samples(
                tpcc_threads,
                pipeline_id_base,
                self.output_dir.join("table_streaming.json"),
            )?);
            print_done("Table streaming", "finished");
        } else {
            print_skip("Table streaming", "disabled by flag");
        }

        print_combined_summary(table_copy_report.as_ref(), table_streaming_report.as_ref());
        self.write_artifact_summaries(table_copy_report.as_ref(), table_streaming_report.as_ref())?;

        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.warehouses == 0 {
            bail!("--warehouses must be greater than 0");
        }

        if self.skip_table_copy && self.skip_table_streaming {
            bail!("at least one benchmark must run");
        }

        if self.samples == 0 {
            bail!("--samples must be greater than 0");
        }

        if self.streaming_drain_quiet_ms == 0 {
            bail!("--streaming-drain-quiet-ms must be greater than 0");
        }

        if self.batch_max_fill_ms == 0 {
            bail!("--batch-max-fill-ms must be greater than 0");
        }

        if self.null_flush_delay_min_ms > self.null_flush_delay_max_ms {
            bail!(
                "--null-flush-delay-min-ms must be less than or equal to --null-flush-delay-max-ms"
            );
        }

        if !(0.0..=1.0).contains(&self.memory_budget_ratio) || self.memory_budget_ratio == 0.0 {
            bail!("--memory-budget-ratio must be greater than 0 and at most 1");
        }

        if self.max_table_sync_workers == 0 {
            bail!("--max-table-sync-workers must be greater than 0");
        }

        if self.max_copy_connections_per_table == 0 {
            bail!("--max-copy-connections-per-table must be greater than 0");
        }

        if let Some(tpcc_threads) = self.tpcc_threads
            && tpcc_threads == 0
        {
            bail!("--tpcc-threads must be greater than 0");
        }

        if let Some(duration_seconds) = self.streaming_duration_seconds
            && duration_seconds == 0
        {
            bail!("--streaming-duration-seconds must be greater than 0");
        }

        for benchmark in &self.hotpath_mcp_benchmarks {
            match benchmark.as_str() {
                "all" | "table_copy" | "table_streaming" => {}
                other => bail!(
                    "unsupported --hotpath-mcp-benchmarks value '{other}'; expected table_copy, \
                     table_streaming, or all"
                ),
            }
        }

        if matches!(self.destination, Destination::BigQuery) {
            if self.bq_project_id.as_deref().is_none_or(|project_id| project_id.trim().is_empty()) {
                bail!("--bq-project-id is required for --destination bigquery");
            }

            if self.bq_dataset_id.as_deref().is_none_or(|dataset_id| dataset_id.trim().is_empty()) {
                bail!("--bq-dataset-id is required for --destination bigquery");
            }

            let Some(sa_key_file) = &self.bq_sa_key_file else {
                bail!("--bq-sa-key-file is required for --destination bigquery");
            };

            if !sa_key_file.exists() {
                bail!(
                    "BigQuery service account key file does not exist: {}",
                    sa_key_file.display()
                );
            }
        }

        if matches!(self.destination, Destination::ClickHouse) {
            if self.clickhouse_url.as_deref().is_none_or(|u| u.trim().is_empty()) {
                bail!("--clickhouse-url is required for --destination clickhouse");
            }

            if self.clickhouse_user.as_deref().is_none_or(|u| u.trim().is_empty()) {
                bail!("--clickhouse-user is required for --destination clickhouse");
            }

            if self.clickhouse_database.as_deref().is_none_or(|d| d.trim().is_empty()) {
                bail!("--clickhouse-database is required for --destination clickhouse");
            }
        }

        if matches!(self.destination, Destination::Snowflake) {
            let connection = std::env::var(BENCH_SNOWFLAKE_CONNECTION_ENV).ok();
            if connection.as_deref().is_none_or(|connection| connection.trim().is_empty()) {
                bail!("{BENCH_SNOWFLAKE_CONNECTION_ENV} is required for --destination snowflake");
            }
        }

        Ok(())
    }

    fn ensure_database(&self) -> Result<()> {
        let exists = self.psql_query(
            "postgres",
            &format!("select 1 from pg_database where datname = {}", quote_literal(&self.database)),
        )?;

        if exists.trim() == "1" {
            return Ok(());
        }

        self.psql_status(
            "postgres",
            &format!("create database {}", quote_identifier(&self.database)),
        )
        .context("failed to create benchmark database")
    }

    fn prepare_tpcc(&self, threads: u16) -> Result<()> {
        if self.force_prepare {
            self.drop_tpcc_tables()?;
        }

        if self.tpcc_tables_exist()? {
            print_skip("TPC-C data", "tables already exist");
            return Ok(());
        }

        print_phase(
            "TPC-C data",
            &format!("loading {} warehouses with {threads} threads", self.warehouses),
        );

        let output = Command::new("go-tpc")
            .args(["tpcc", "--warehouses", &self.warehouses.to_string(), "prepare"])
            .args(["-d", "postgres"])
            .args(["-U", &self.username])
            .args(["-p", &self.password])
            .args(["-D", &self.database])
            .args(["-H", &self.host])
            .args(["-P", &self.port.to_string()])
            .args(["--conn-params", "sslmode=disable"])
            .args(["-T", &threads.to_string()])
            .arg("--no-check")
            .output()
            .context("failed to run go-tpc")?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stdout.trim().is_empty() {
                eprintln!("go-tpc prepare stdout:");
                eprint!("{stdout}");
            }
            if !stderr.trim().is_empty() {
                eprintln!("go-tpc prepare stderr:");
                eprint!("{stderr}");
            }
            bail!("go-tpc prepare failed");
        }

        Ok(())
    }

    fn drop_tpcc_tables(&self) -> Result<()> {
        self.psql_status(
            &self.database,
            "drop table if exists customer, district, history, item, new_order, order_line, \
             orders, stock, warehouse cascade",
        )
        .context("failed to drop existing TPC-C tables")
    }

    fn tpcc_tables_exist(&self) -> Result<bool> {
        let tables = self.validated_tpcc_tables()?;
        self.tpcc_tables_exist_for(&tables)
    }

    fn tpcc_tables_exist_for(&self, tables: &[String]) -> Result<bool> {
        let names = tables.iter().map(|table| quote_literal(table)).collect::<Vec<_>>().join(",");
        let count = self.psql_query(
            &self.database,
            &format!(
                "select count(*) from pg_catalog.pg_tables where schemaname = 'public' and \
                 tablename in ({names})"
            ),
        )?;
        Ok(count.trim().parse::<usize>()? == tables.len())
    }

    fn ensure_tpcc_tables_exist(&self) -> Result<()> {
        let tables = self.validated_tpcc_tables()?;
        if self.tpcc_tables_exist_for(&tables)? {
            return Ok(());
        }

        bail!(
            "TPC-C tables are missing in database '{}'; rerun without --skip-prepare or pass \
             --force-prepare",
            self.database
        )
    }

    fn validated_tpcc_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::with_capacity(self.tpcc_tables.len());
        for table in &self.tpcc_tables {
            match table.as_str() {
                "customer" | "district" | "item" | "new_order" | "order_line" | "orders"
                | "stock" | "warehouse" => tables.push(table.clone()),
                _ => bail!(
                    "unsupported TPC-C table '{table}'; supported tables are {DEFAULT_TPCC_TABLES}"
                ),
            }
        }

        Ok(tables)
    }

    fn fetch_table_ids(&self, tables: &[String]) -> Result<String> {
        let names = tables.iter().map(|table| quote_literal(table)).collect::<Vec<_>>().join(",");
        let table_ids = self.psql_query(
            &self.database,
            &format!(
                "select string_agg(c.oid::text, ',' order by c.relname) from pg_class c join \
                 pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and \
                 c.relname in ({names}) and c.relkind = 'r'"
            ),
        )?;
        let table_ids = table_ids.trim().to_owned();
        if table_ids.is_empty() {
            bail!("could not find selected TPC-C table IDs");
        }

        Ok(table_ids)
    }

    fn fetch_expected_row_count(&self, tables: &[String]) -> Result<u64> {
        let query = tables
            .iter()
            .map(|table| format!("(select count(*) from {})", quote_identifier(table)))
            .collect::<Vec<_>>()
            .join(" + ");
        let count = self.psql_query(&self.database, &format!("select {query}"))?;

        Ok(count.trim().parse()?)
    }

    fn create_publication(&self, publication_name: &str, tables: &[String]) -> Result<()> {
        let publication_name = quote_identifier(publication_name);
        let table_list = tables
            .iter()
            .map(|table| quote_identifier(table).into_owned())
            .collect::<Vec<_>>()
            .join(", ");
        self.psql_status(
            &self.database,
            &format!(
                "drop publication if exists {publication_name}; create publication \
                 {publication_name} for table {table_list};"
            ),
        )
        .context("failed to create benchmark publication")
    }

    fn drop_publication(&self, publication_name: &str) -> Result<()> {
        let publication_name = quote_identifier(publication_name);
        self.psql_status(&self.database, &format!("drop publication if exists {publication_name}"))
            .context("failed to drop benchmark publication")
    }

    fn finish_sample_with_publication(
        &self,
        publication_name: &str,
        report: Result<Value>,
    ) -> Result<Value> {
        match report {
            Ok(report) => {
                self.drop_publication(publication_name)?;
                Ok(report)
            }
            Err(error) => {
                if let Err(cleanup_error) = self.drop_publication(publication_name) {
                    eprintln!("failed to clean up benchmark publication: {cleanup_error:#}");
                }
                Err(error)
            }
        }
    }

    fn run_table_copy_samples(
        &self,
        selected_tables: &[String],
        table_ids: &str,
        expected_row_count: u64,
        pipeline_id_base: u64,
        report_path: PathBuf,
    ) -> Result<Value> {
        let mut measured_reports = Vec::with_capacity(usize::from(self.samples));
        let mut measured_hotpath_report_paths = Vec::with_capacity(usize::from(self.samples));
        let total_samples = self.total_sample_count();
        for sample_idx in 0..total_samples {
            let pipeline_id = sample_pipeline_id(pipeline_id_base, 0, sample_idx)?;
            let publication_name = format!("bench_pub_{pipeline_id}");
            let sample_report_path =
                self.sample_report_path("table_copy", sample_idx.saturating_add(1));
            let sample_hotpath_report_path =
                self.hotpath_report_path_for_report("table_copy", &sample_report_path);
            let sample_label = self.sample_label(sample_idx);

            print_phase("Table copy", &format!("{sample_label} creating publication"));
            self.create_publication(&publication_name, selected_tables)?;
            print_phase("Table copy", &format!("{sample_label} running"));
            let report = self.run_table_copy(
                &publication_name,
                table_ids,
                expected_row_count,
                pipeline_id,
                sample_report_path.clone(),
            );
            let report = self.finish_sample_with_publication(&publication_name, report)?;
            remove_sample_report(&sample_report_path)?;
            print_sample_result("table_copy", &sample_label, &report);

            if self.is_warmup_sample(sample_idx) {
                print_skip("Table copy", &format!("{sample_label} discarded as warmup"));
            } else {
                measured_reports.push(report);
                measured_hotpath_report_paths.push(sample_hotpath_report_path);
            }
        }

        let aggregate = aggregate_reports(
            "table_copy",
            usize::from(self.samples),
            usize::from(self.warmup_samples),
            &measured_reports,
        )?;
        self.copy_selected_hotpath_report(
            "table_copy",
            &measured_reports,
            &measured_hotpath_report_paths,
        )?;
        write_json_report(&aggregate, &report_path)?;
        print_benchmark_report("table_copy", &aggregate);
        print_done("Report", &format!("wrote {}", report_path.display()));
        Ok(aggregate)
    }

    fn run_table_copy(
        &self,
        publication_name: &str,
        table_ids: &str,
        expected_row_count: u64,
        pipeline_id: u64,
        report_path: PathBuf,
    ) -> Result<Value> {
        let mut args = vec!["--log-target".to_owned(), "terminal".to_owned(), "run".to_owned()];
        self.push_common_benchmark_args(&mut args);
        self.push_tuning_args(&mut args);
        self.push_destination_args(&mut args)?;
        args.extend([
            "--report-path".to_owned(),
            report_path.display().to_string(),
            "--pipeline-id".to_owned(),
            pipeline_id.to_string(),
            "--publication-name".to_owned(),
            publication_name.to_owned(),
            "--table-ids".to_owned(),
            table_ids.to_owned(),
            "--expected-row-count".to_owned(),
            expected_row_count.to_string(),
        ]);

        run_benchmark_binary(
            "table_copy",
            self.destination,
            &self.benchmark_profile,
            self.hotpath_run_config("table_copy", &report_path),
            &args,
            &report_path,
        )
    }

    fn run_table_streaming_samples(
        &self,
        tpcc_threads: u16,
        pipeline_id_base: u64,
        report_path: PathBuf,
    ) -> Result<Value> {
        let selected_tables = self.validated_tpcc_tables()?;
        let mut measured_reports = Vec::with_capacity(usize::from(self.samples));
        let mut measured_hotpath_report_paths = Vec::with_capacity(usize::from(self.samples));
        let total_samples = self.total_sample_count();
        for sample_idx in 0..total_samples {
            let pipeline_id = sample_pipeline_id(pipeline_id_base, 1, sample_idx)?;
            let publication_name = format!("bench_streaming_pub_{pipeline_id}");
            let sample_report_path =
                self.sample_report_path("table_streaming", sample_idx.saturating_add(1));
            let sample_hotpath_report_path =
                self.hotpath_report_path_for_report("table_streaming", &sample_report_path);
            let sample_label = self.sample_label(sample_idx);

            print_phase("Table streaming", &format!("{sample_label} preparing"));
            let report = self.run_table_streaming(
                &publication_name,
                tpcc_threads,
                pipeline_id,
                &selected_tables,
                sample_report_path.clone(),
            );
            let report = self.finish_sample_with_publication(&publication_name, report)?;
            remove_sample_report(&sample_report_path)?;
            print_sample_result("table_streaming", &sample_label, &report);

            if self.is_warmup_sample(sample_idx) {
                print_skip("Table streaming", &format!("{sample_label} discarded as warmup"));
            } else {
                measured_reports.push(report);
                measured_hotpath_report_paths.push(sample_hotpath_report_path);
            }
        }

        let aggregate = aggregate_reports(
            "table_streaming",
            usize::from(self.samples),
            usize::from(self.warmup_samples),
            &measured_reports,
        )?;
        self.copy_selected_hotpath_report(
            "table_streaming",
            &measured_reports,
            &measured_hotpath_report_paths,
        )?;
        write_json_report(&aggregate, &report_path)?;
        print_benchmark_report("table_streaming", &aggregate);
        print_done("Report", &format!("wrote {}", report_path.display()));
        Ok(aggregate)
    }

    fn run_table_streaming(
        &self,
        publication_name: &str,
        tpcc_threads: u16,
        pipeline_id: u64,
        selected_tables: &[String],
        report_path: PathBuf,
    ) -> Result<Value> {
        let mut args = vec!["--log-target".to_owned(), "terminal".to_owned(), "run".to_owned()];
        self.push_common_benchmark_args(&mut args);
        self.push_tuning_args(&mut args);
        self.push_destination_args(&mut args)?;
        args.extend([
            "--report-path".to_owned(),
            report_path.display().to_string(),
            "--pipeline-id".to_owned(),
            pipeline_id.to_string(),
            "--publication-name".to_owned(),
            publication_name.to_owned(),
            "--tpcc-warehouses".to_owned(),
            self.warehouses.to_string(),
            "--tpcc-threads".to_owned(),
            tpcc_threads.to_string(),
            "--drain-quiet-ms".to_owned(),
            self.streaming_drain_quiet_ms.to_string(),
        ]);

        let table_ids = self.fetch_table_ids(selected_tables)?;
        self.create_publication(publication_name, selected_tables)?;
        let duration_seconds =
            self.streaming_duration_seconds.unwrap_or(DEFAULT_STREAMING_DURATION_SECONDS);
        print_phase(
            "Table streaming",
            &format!(
                "generating TPC-C transactions for {duration_seconds}s across {} tables",
                selected_tables.len()
            ),
        );
        args.extend([
            "--table-ids".to_owned(),
            table_ids,
            "--duration-seconds".to_owned(),
            duration_seconds.to_string(),
        ]);

        run_benchmark_binary(
            "table_streaming",
            self.destination,
            &self.benchmark_profile,
            self.hotpath_run_config("table_streaming", &report_path),
            &args,
            &report_path,
        )
    }

    fn total_sample_count(&self) -> usize {
        usize::from(self.samples) + usize::from(self.warmup_samples)
    }

    fn is_warmup_sample(&self, sample_idx: usize) -> bool {
        sample_idx < usize::from(self.warmup_samples)
    }

    fn sample_label(&self, sample_idx: usize) -> String {
        let warmups = usize::from(self.warmup_samples);
        if sample_idx < warmups {
            format!("warmup {}/{}", sample_idx + 1, warmups)
        } else {
            format!("sample {}/{}", sample_idx + 1 - warmups, self.samples)
        }
    }

    fn sample_report_path(&self, benchmark: &str, sample_number: usize) -> PathBuf {
        self.output_dir.join(format!(".{benchmark}_sample_{sample_number}.json"))
    }

    fn push_common_benchmark_args(&self, args: &mut Vec<String>) {
        args.extend([
            "--host".to_owned(),
            self.host.clone(),
            "--port".to_owned(),
            self.port.to_string(),
            "--database".to_owned(),
            self.database.clone(),
            "--username".to_owned(),
            self.username.clone(),
            "--password".to_owned(),
            self.password.clone(),
        ]);
    }

    fn push_tuning_args(&self, args: &mut Vec<String>) {
        args.extend([
            "--batch-max-fill-ms".to_owned(),
            self.batch_max_fill_ms.to_string(),
            "--null-flush-delay-min-ms".to_owned(),
            self.null_flush_delay_min_ms.to_string(),
            "--null-flush-delay-max-ms".to_owned(),
            self.null_flush_delay_max_ms.to_string(),
            "--memory-budget-ratio".to_owned(),
            self.memory_budget_ratio.to_string(),
            "--max-table-sync-workers".to_owned(),
            self.max_table_sync_workers.to_string(),
            "--max-copy-connections-per-table".to_owned(),
            self.max_copy_connections_per_table.to_string(),
        ]);

        if !self.enable_memory_backpressure {
            args.push("--disable-memory-backpressure".to_owned());
        }
    }

    fn push_destination_args(&self, args: &mut Vec<String>) -> Result<()> {
        args.extend(["--destination".to_owned(), self.destination.as_arg().to_owned()]);

        match self.destination {
            Destination::BigQuery => {
                if let Some(project_id) = &self.bq_project_id {
                    args.extend(["--bq-project-id".to_owned(), project_id.clone()]);
                }

                if let Some(dataset_id) = &self.bq_dataset_id {
                    args.extend(["--bq-dataset-id".to_owned(), dataset_id.clone()]);
                }

                if let Some(sa_key_file) = &self.bq_sa_key_file {
                    args.extend(["--bq-sa-key-file".to_owned(), sa_key_file.display().to_string()]);
                }
            }
            Destination::ClickHouse => {
                if let Some(url) = &self.clickhouse_url {
                    args.extend(["--clickhouse-url".to_owned(), url.clone()]);
                }

                if let Some(user) = &self.clickhouse_user {
                    args.extend(["--clickhouse-user".to_owned(), user.clone()]);
                }

                if let Some(password) = &self.clickhouse_password {
                    args.extend(["--clickhouse-password".to_owned(), password.clone()]);
                }

                if let Some(database) = &self.clickhouse_database {
                    args.extend(["--clickhouse-database".to_owned(), database.clone()]);
                }
            }
            Destination::Snowflake => {}
            Destination::Null => {}
        }

        Ok(())
    }

    fn psql_query(&self, database: &str, sql: &str) -> Result<String> {
        let output = self.psql_output(database, sql)?;
        if !output.status.success() {
            bail!("psql query failed: {}", String::from_utf8_lossy(&output.stderr).trim());
        }

        Ok(String::from_utf8(output.stdout)?)
    }

    fn psql_status(&self, database: &str, sql: &str) -> Result<()> {
        let output = self.psql_output(database, sql)?;
        if !output.status.success() {
            bail!("psql command failed: {}", String::from_utf8_lossy(&output.stderr).trim());
        }

        Ok(())
    }

    fn psql_output(&self, database: &str, sql: &str) -> Result<Output> {
        let mut command = Command::new("psql");
        command
            .args(["-v", "ON_ERROR_STOP=1"])
            .args(["-h", &self.host])
            .args(["-U", &self.username])
            .args(["-p", &self.port.to_string()])
            .args(["-d", database])
            .args(["-tAc", sql])
            .env("PGPASSWORD", &self.password);

        command.output().context("failed to run psql")
    }

    fn runs_tpcc_streaming(&self) -> bool {
        !self.skip_table_streaming
    }

    fn needs_tpcc_tables(&self) -> bool {
        !self.skip_table_copy || self.runs_tpcc_streaming()
    }

    fn hotpath_enabled_for_any_benchmark(&self) -> bool {
        self.hotpath
            || self.hotpath_alloc
            || self.hotpath_cpu
            || !self.hotpath_mcp_benchmarks.is_empty()
            || self.hotpath_output_dir.is_some()
    }

    fn effective_hotpath_output_dir(&self) -> Option<PathBuf> {
        self.hotpath_enabled_for_any_benchmark().then(|| {
            self.hotpath_output_dir.clone().unwrap_or_else(|| self.output_dir.join("hotpath"))
        })
    }

    fn hotpath_mcp_enabled_for(&self, benchmark: &str) -> bool {
        self.hotpath_mcp_benchmarks.iter().any(|value| value == "all" || value == benchmark)
    }

    fn hotpath_run_config(&self, benchmark: &str, report_path: &Path) -> HotpathRunConfig {
        let mcp_enabled = self.hotpath_mcp_enabled_for(benchmark);
        let enabled = self.hotpath
            || self.hotpath_alloc
            || self.hotpath_cpu
            || mcp_enabled
            || self.hotpath_output_dir.is_some();
        let output_path =
            enabled.then(|| self.hotpath_report_path_for_report(benchmark, report_path)).flatten();

        HotpathRunConfig {
            enabled,
            alloc: self.hotpath_alloc,
            cpu: self.hotpath_cpu,
            mcp: mcp_enabled,
            mcp_port: self.hotpath_mcp_port,
            output_path,
        }
    }

    fn hotpath_report_path_for_report(
        &self,
        benchmark: &str,
        report_path: &Path,
    ) -> Option<PathBuf> {
        self.effective_hotpath_output_dir().map(|dir| {
            let file_name = report_path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .map_or_else(|| format!("{benchmark}.json"), ToOwned::to_owned);
            dir.join(file_name)
        })
    }

    fn canonical_hotpath_report_path(&self, benchmark: &str) -> Option<PathBuf> {
        self.effective_hotpath_output_dir().map(|dir| dir.join(format!("{benchmark}.json")))
    }

    fn copy_selected_hotpath_report(
        &self,
        benchmark: &str,
        measured_reports: &[Value],
        measured_hotpath_report_paths: &[Option<PathBuf>],
    ) -> Result<()> {
        let Some(canonical_path) = self.canonical_hotpath_report_path(benchmark) else {
            return Ok(());
        };
        let Some(selected_idx) = selected_report_index(benchmark, measured_reports) else {
            return Ok(());
        };
        let Some(source_path) =
            measured_hotpath_report_paths.get(selected_idx).and_then(Option::as_deref)
        else {
            return Ok(());
        };
        if !source_path.exists() {
            return Ok(());
        }

        if let Some(parent) = canonical_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create HotPath report directory {}", parent.display())
            })?;
        }

        fs::copy(source_path, &canonical_path).with_context(|| {
            format!(
                "failed to copy selected HotPath report from {} to {}",
                source_path.display(),
                canonical_path.display()
            )
        })?;
        Ok(())
    }

    fn hotpath_label(&self) -> String {
        if !self.hotpath_enabled_for_any_benchmark() {
            return "disabled".to_owned();
        }

        let mut modes = Vec::new();
        if self.hotpath {
            modes.push("timing");
        }
        if self.hotpath_alloc {
            modes.push("alloc");
        }
        if self.hotpath_cpu {
            modes.push("cpu");
        }
        if !self.hotpath_mcp_benchmarks.is_empty() {
            modes.push("mcp");
        }
        if modes.is_empty() {
            modes.push("timing");
        }

        let output_dir = self
            .effective_hotpath_output_dir()
            .map_or_else(|| "stdout".to_owned(), |path| path.display().to_string());
        let mut label = format!("{} ({output_dir})", modes.join(","));
        if !self.hotpath_mcp_benchmarks.is_empty() {
            label.push_str(&format!(", mcp=http://localhost:{}/mcp", self.hotpath_mcp_port));
        }
        label
    }

    fn null_flush_delay_label(&self) -> String {
        match (self.null_flush_delay_min_ms, self.null_flush_delay_max_ms) {
            (0, 0) => "disabled".to_owned(),
            (min_ms, max_ms) if min_ms == max_ms => format!("{min_ms} ms"),
            (min_ms, max_ms) => format!("{min_ms}-{max_ms} ms"),
        }
    }

    fn write_artifact_summaries(
        &self,
        table_copy_report: Option<&Value>,
        table_streaming_report: Option<&Value>,
    ) -> Result<()> {
        let mut summary = json!({
            "output_dir": self.output_dir,
            "hotpath_output_dir": self.effective_hotpath_output_dir(),
            "benchmarks": [
                self.benchmark_artifact_summary("table_copy", table_copy_report)?,
                self.benchmark_artifact_summary("table_streaming", table_streaming_report)?,
            ],
        });

        let cpu_profile_copies = self.copy_cpu_profiles(&summary)?;
        if let Some(summary) = summary.as_object_mut() {
            summary.insert("cpu_profile_copies".to_owned(), cpu_profile_copies);
        }

        let json_path = self.output_dir.join("benchmark_artifacts.json");
        write_json_report(&summary, &json_path)?;

        let markdown_path = self.output_dir.join("benchmark_artifacts.md");
        write_markdown_report(&render_artifact_markdown(&summary), &markdown_path)?;

        print_done(
            "Artifacts",
            &format!("wrote {} and {}", json_path.display(), markdown_path.display()),
        );

        Ok(())
    }

    fn benchmark_artifact_summary(&self, benchmark: &str, report: Option<&Value>) -> Result<Value> {
        let report_path = report.map(|_| self.output_dir.join(format!("{benchmark}.json")));
        let hotpath_report_path =
            report.and_then(|_| self.canonical_hotpath_report_path(benchmark)).as_ref().cloned();
        let hotpath_sample_report_paths = report
            .and_then(|_| self.hotpath_sample_report_paths(benchmark).transpose())
            .transpose()?;
        let hotpath_report = report
            .and_then(|_| self.canonical_hotpath_report_path(benchmark))
            .as_deref()
            .and_then(read_report_if_exists);

        Ok(json!({
            "benchmark": benchmark,
            "ran": report.is_some(),
            "benchmark_report_path": report_path,
            "hotpath_report_path": hotpath_report_path,
            "hotpath_sample_report_paths": hotpath_sample_report_paths,
            "benchmark_report": summarize_benchmark_report(report),
            "hotpath_report": summarize_hotpath_report(hotpath_report.as_ref()),
        }))
    }

    fn copy_cpu_profiles(&self, summary: &Value) -> Result<Value> {
        let Some(benchmarks) = summary.get("benchmarks").and_then(Value::as_array) else {
            return Ok(Value::Array(Vec::new()));
        };

        let mut copied_profiles = Vec::new();
        for benchmark in benchmarks {
            let benchmark_name =
                benchmark.get("benchmark").and_then(Value::as_str).unwrap_or("unknown");
            let Some(source_path) = benchmark
                .get("hotpath_report")
                .and_then(|hotpath| hotpath.get("flamegraph"))
                .and_then(|flamegraph| flamegraph.get("samply_profile_path"))
                .and_then(Value::as_str)
            else {
                continue;
            };

            let source_path = PathBuf::from(source_path);
            let mut profile_copy = json!({
                "benchmark": benchmark_name,
                "source_path": source_path,
            });

            if source_path.is_file() {
                let cpu_profiles_dir = self.output_dir.join("cpu-profiles");
                fs::create_dir_all(&cpu_profiles_dir).with_context(|| {
                    format!("failed to create CPU profile directory {}", cpu_profiles_dir.display())
                })?;

                let file_name = source_path
                    .file_name()
                    .and_then(|file_name| file_name.to_str())
                    .unwrap_or("hp.json.gz");
                let run_id = source_path
                    .parent()
                    .and_then(|path| path.file_name())
                    .and_then(|file_name| file_name.to_str())
                    .unwrap_or("unknown-run");
                let copied_path =
                    cpu_profiles_dir.join(format!("{benchmark_name}-{run_id}-{file_name}"));

                fs::copy(&source_path, &copied_path).with_context(|| {
                    format!(
                        "failed to copy CPU profile {} to {}",
                        source_path.display(),
                        copied_path.display()
                    )
                })?;

                if let Some(profile_copy) = profile_copy.as_object_mut() {
                    profile_copy.insert("status".to_owned(), Value::String("copied".to_owned()));
                    profile_copy.insert("copied_path".to_owned(), json!(copied_path));
                    profile_copy.insert(
                        "open_command".to_owned(),
                        Value::String(format!("samply load {}", copied_path.display())),
                    );

                    match write_cpu_sample_summary(benchmark_name, &copied_path) {
                        Ok(Some(summary)) => {
                            profile_copy.insert(
                                "sample_summary_status".to_owned(),
                                Value::String("ok".to_owned()),
                            );
                            profile_copy.insert("sample_summary".to_owned(), summary);
                        }
                        Ok(None) => {
                            profile_copy.insert(
                                "sample_summary_status".to_owned(),
                                Value::String("not_available".to_owned()),
                            );
                        }
                        Err(error) => {
                            profile_copy.insert(
                                "sample_summary_status".to_owned(),
                                Value::String("failed".to_owned()),
                            );
                            profile_copy.insert(
                                "sample_summary_error".to_owned(),
                                Value::String(error.to_string()),
                            );
                        }
                    }
                }
            } else if let Some(profile_copy) = profile_copy.as_object_mut() {
                profile_copy.insert("status".to_owned(), Value::String("missing".to_owned()));
            }

            copied_profiles.push(profile_copy);
        }

        Ok(Value::Array(copied_profiles))
    }

    fn hotpath_sample_report_paths(&self, benchmark: &str) -> Result<Option<Vec<PathBuf>>> {
        let Some(output_dir) = self.effective_hotpath_output_dir() else {
            return Ok(None);
        };
        if !output_dir.exists() {
            return Ok(Some(Vec::new()));
        }

        let prefix = format!(".{benchmark}_sample_");
        let mut paths = Vec::new();
        for entry in fs::read_dir(&output_dir).with_context(|| {
            format!("failed to read HotPath output dir {}", output_dir.display())
        })? {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|file_name| file_name.to_str()) else {
                continue;
            };
            if file_name.starts_with(&prefix) {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(Some(paths))
    }
}

#[derive(Clone, Debug)]
struct HotpathRunConfig {
    enabled: bool,
    alloc: bool,
    cpu: bool,
    mcp: bool,
    mcp_port: u16,
    output_path: Option<PathBuf>,
}

impl Destination {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::BigQuery => "bigquery",
            Self::ClickHouse => "clickhouse",
            Self::Snowflake => "snowflake",
        }
    }
}

fn recommended_threads(warehouses: u16) -> u16 {
    let warehouse_threads =
        usize::from(warehouses).saturating_mul(DEFAULT_TPCC_THREADS_PER_WAREHOUSE);
    let cpu_threads = std::thread::available_parallelism()
        .map_or(DEFAULT_TPCC_MIN_THREADS, |parallelism| {
            parallelism.get().saturating_mul(DEFAULT_TPCC_THREADS_PER_CPU)
        });
    warehouse_threads.max(cpu_threads).clamp(DEFAULT_TPCC_MIN_THREADS, DEFAULT_TPCC_MAX_THREADS)
        as u16
}

fn memory_backpressure_label(enabled: bool) -> String {
    if enabled {
        format!(
            "enabled (activate {:.0}%, resume {:.0}%)",
            MEMORY_BACKPRESSURE_ACTIVATE_THRESHOLD * 100.0,
            MEMORY_BACKPRESSURE_RESUME_THRESHOLD * 100.0
        )
    } else {
        "disabled".to_owned()
    }
}

fn benchmark_pipeline_id_base() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_micros()
        .try_into()
        .context("current timestamp does not fit in a pipeline id")
}

fn check_command(command: &str) -> Result<()> {
    let mut cmd = Command::new(command);
    if command == "go-tpc" {
        cmd.arg("version");
    } else {
        cmd.arg("--version");
    }

    let status = cmd.stdout(Stdio::null()).stderr(Stdio::null()).status();

    if status.is_ok_and(|status| status.success()) {
        return Ok(());
    }

    bail!("required command '{command}' was not found")
}

fn sample_pipeline_id(base: u64, benchmark_offset: u64, sample_idx: usize) -> Result<u64> {
    let sample_offset = u64::try_from(sample_idx)
        .context("benchmark sample index does not fit in pipeline id")?
        .checked_mul(2)
        .context("benchmark sample pipeline id overflow")?;
    base.checked_add(sample_offset)
        .and_then(|id| id.checked_add(benchmark_offset))
        .context("benchmark pipeline id overflow")
}

fn remove_sample_report(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn write_json_report(report: &Value, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }

    let json = serde_json::to_string_pretty(report)?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("failed to write benchmark report to {}", path.display()))
}

fn write_markdown_report(markdown: &str, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }

    fs::write(path, markdown)
        .with_context(|| format!("failed to write benchmark summary to {}", path.display()))
}

fn read_report_if_exists(path: &Path) -> Option<Value> {
    fs::read_to_string(path).ok().and_then(|contents| serde_json::from_str(&contents).ok())
}

fn summarize_benchmark_report(report: Option<&Value>) -> Value {
    let Some(report) = report else {
        return json!({"status": "not_run"});
    };

    json!({
        "status": "ok",
        "benchmark": report.get("benchmark"),
        "rows_copied": report.get("copied_rows"),
        "rows_per_second": report.get("rows_per_second"),
        "produced_events": report.get("produced_events"),
        "observed_cdc_events": report.get("observed_cdc_events"),
        "events_per_second": report.get("end_to_end_with_shutdown_events_per_second"),
        "decoded_mib_per_second": report.get("estimated_mib_per_second").or_else(|| report.get("decoded_mib_per_second")),
        "total_ms": report.get("total_ms"),
        "stage_breakdown": benchmark_stage_breakdown(report),
        "sample_summary": report.get("sample_summary"),
        "null_flush_delay_min_ms": report.get("null_flush_delay_min_ms"),
        "null_flush_delay_max_ms": report.get("null_flush_delay_max_ms"),
        "tokio_runtime_stats": report.get("tokio_runtime_stats"),
        "destination_stats": report.get("destination_stats"),
    })
}

fn benchmark_stage_breakdown(report: &Value) -> Value {
    match report.get("benchmark").and_then(Value::as_str) {
        Some("table_copy") => json!({
            "pipeline_start_ms": report.get("pipeline_start_ms"),
            "copy_wait_ms": report.get("copy_wait_ms"),
            "shutdown_ms": report.get("shutdown_ms"),
            "total_ms": report.get("total_ms"),
        }),
        Some("table_streaming") => json!({
            "pipeline_start_ms": report.get("pipeline_start_ms"),
            "ready_wait_ms": report.get("ready_wait_ms"),
            "producer_ms": report.get("producer_ms"),
            "drain_ms": report.get("drain_ms"),
            "end_to_end_ms": report.get("end_to_end_ms"),
            "shutdown_ms": report.get("shutdown_ms"),
            "end_to_end_with_shutdown_ms": report.get("end_to_end_with_shutdown_ms"),
            "total_ms": report.get("total_ms"),
        }),
        _ => Value::Null,
    }
}

fn summarize_hotpath_report(report: Option<&Value>) -> Value {
    let Some(report) = report else {
        return json!({"status": "missing"});
    };

    let cpu_profile_path = find_string_key(report, "profile_path");
    let cpu_message = report
        .get("functions_cpu")
        .and_then(|section| section.get("message"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let flamegraph_status = match (&cpu_profile_path, &cpu_message) {
        (Some(_), _) => "available",
        (None, Some(_)) => "failed",
        (None, None) if report.get("functions_cpu").is_some() => "empty",
        (None, None) => "not_requested",
    };

    json!({
        "status": "ok",
        "sections": {
            "functions_timing": summarize_hotpath_section(report, "functions_timing"),
            "functions_alloc": summarize_hotpath_section(report, "functions_alloc"),
            "functions_cpu": summarize_hotpath_section(report, "functions_cpu"),
            "futures": summarize_hotpath_section(report, "futures"),
            "mutexes": summarize_hotpath_section(report, "mutexes"),
            "rw_locks": summarize_hotpath_section(report, "rw_locks"),
            "sql": summarize_hotpath_section(report, "sql"),
            "threads": summarize_hotpath_section(report, "threads"),
            "cpu_baseline": summarize_hotpath_section(report, "cpu_baseline"),
        },
        "flamegraph": {
            "status": flamegraph_status,
            "samply_profile_path": cpu_profile_path,
            "open_command": cpu_profile_path.as_ref().map(|path| format!("samply load {path}")),
            "message": cpu_message,
        },
    })
}

fn summarize_hotpath_section(report: &Value, section_name: &str) -> Value {
    let Some(section) = report.get(section_name) else {
        return json!({"status": "missing", "count": 0});
    };

    let data = section.get("data").and_then(Value::as_array);
    let top =
        data.map(|rows| rows.iter().take(10).cloned().collect::<Vec<_>>()).unwrap_or_default();

    json!({
        "status": "ok",
        "count": data.map_or(0, Vec::len),
        "message": section.get("message"),
        "top": top,
    })
}

fn write_cpu_sample_summary(benchmark_name: &str, profile_path: &Path) -> Result<Option<Value>> {
    let Some(profile_json) = read_gzip_json(profile_path)? else {
        return Ok(None);
    };
    let Some(summary) = summarize_cpu_profile_samples(benchmark_name, &profile_json) else {
        return Ok(None);
    };

    let summary_path = profile_path.with_extension("sample-summary.json");
    write_json_report(&summary, &summary_path)?;

    let markdown_path = profile_path.with_extension("sample-summary.md");
    write_markdown_report(&render_cpu_sample_summary_markdown(&summary), &markdown_path)?;

    Ok(Some(json!({
        "json_path": summary_path,
        "markdown_path": markdown_path,
    })))
}

fn read_gzip_json(path: &Path) -> Result<Option<Value>> {
    let output = Command::new("gzip")
        .arg("-dc")
        .arg(path)
        .output()
        .with_context(|| format!("failed to decompress CPU profile {}", path.display()))?;

    if !output.status.success() {
        return Ok(None);
    }

    let profile = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed to parse CPU profile {}", path.display()))?;
    Ok(Some(profile))
}

fn summarize_cpu_profile_samples(benchmark_name: &str, profile: &Value) -> Option<Value> {
    let threads = profile.get("threads").and_then(Value::as_array)?;
    let binary_path = find_profile_binary_path(benchmark_name, profile);
    let mut inclusive_counts = HashMap::<String, u64>::new();
    let mut leaf_counts = HashMap::<String, u64>::new();
    let mut application_inclusive_counts = HashMap::<String, u64>::new();
    let mut application_leaf_counts = HashMap::<String, u64>::new();
    let mut threads_seen = HashSet::<String>::new();
    let mut application_threads_seen = HashSet::<String>::new();
    let mut sample_count = 0_u64;
    let mut application_sample_count = 0_u64;

    for thread in threads {
        let thread_name = thread.get("name").and_then(Value::as_str).unwrap_or("unknown");
        let application_thread = !is_hotpath_internal_thread(thread_name);
        threads_seen.insert(thread_name.to_owned());
        if application_thread {
            application_threads_seen.insert(thread_name.to_owned());
        }

        let Some(sample_stacks) = thread
            .get("samples")
            .and_then(|samples| samples.get("stack"))
            .and_then(Value::as_array)
        else {
            continue;
        };

        for stack in sample_stacks {
            let Some(stack_index) = stack.as_u64().and_then(|stack| usize::try_from(stack).ok())
            else {
                continue;
            };
            sample_count += 1;
            if application_thread {
                application_sample_count += 1;
            }

            let mut stack_functions = profile_stack_functions(thread, stack_index);
            if stack_functions.is_empty() {
                continue;
            }

            if let Some(leaf) = stack_functions.first() {
                *leaf_counts.entry(leaf.clone()).or_default() += 1;
                if application_thread {
                    *application_leaf_counts.entry(leaf.clone()).or_default() += 1;
                }
            }

            stack_functions.sort();
            stack_functions.dedup();
            for function in stack_functions {
                if application_thread {
                    *application_inclusive_counts.entry(function.clone()).or_default() += 1;
                }
                *inclusive_counts.entry(function).or_default() += 1;
            }
        }
    }

    let symbolizer = binary_path.as_deref().map_or_else(
        || {
            json!({
                "status": "missing_binary_path",
                "symbols": {},
            })
        },
        |path| symbolize_cpu_profile_addresses(Path::new(path), inclusive_counts.keys()),
    );
    let symbols = symbolizer.get("symbols").and_then(Value::as_object);

    let mut thread_names = threads_seen.into_iter().collect::<Vec<_>>();
    thread_names.sort();
    let mut application_thread_names = application_threads_seen.into_iter().collect::<Vec<_>>();
    application_thread_names.sort();

    Some(json!({
        "benchmark": benchmark_name,
        "sample_count": sample_count,
        "application_sample_count": application_sample_count,
        "thread_count": thread_names.len(),
        "application_thread_count": application_thread_names.len(),
        "threads": thread_names,
        "application_threads": application_thread_names,
        "binary_path": binary_path,
        "symbolizer": symbolizer,
        "application_filter": {
            "description": "Filtered application tables hide common profiler, symbolication, CLI, and sampling transport frames so ETL symbols are easier to review. Unfiltered tables remain below for auditability.",
            "hidden_patterns": CPU_SAMPLE_SUMMARY_FILTER_PATTERNS,
            "etl_patterns": CPU_SAMPLE_SUMMARY_ETL_PATTERNS,
        },
        "top_etl_application_inclusive": top_etl_cpu_profile_entries(&application_inclusive_counts, symbols, 30),
        "top_etl_application_leaf": top_etl_cpu_profile_entries(&application_leaf_counts, symbols, 30),
        "top_filtered_application_inclusive": top_filtered_cpu_profile_entries(&application_inclusive_counts, symbols, 30),
        "top_filtered_application_leaf": top_filtered_cpu_profile_entries(&application_leaf_counts, symbols, 30),
        "top_application_inclusive": top_cpu_profile_entries(&application_inclusive_counts, symbols, 30),
        "top_application_leaf": top_cpu_profile_entries(&application_leaf_counts, symbols, 30),
        "top_inclusive": top_cpu_profile_entries(&inclusive_counts, symbols, 30),
        "top_leaf": top_cpu_profile_entries(&leaf_counts, symbols, 30),
    }))
}

fn is_hotpath_internal_thread(thread_name: &str) -> bool {
    thread_name.starts_with("hp-")
}

fn find_profile_binary_path(benchmark_name: &str, profile: &Value) -> Option<String> {
    profile
        .get("libs")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|library| library.get("path").and_then(Value::as_str))
        .find(|path| path.ends_with(benchmark_name))
        .map(ToOwned::to_owned)
}

fn profile_stack_functions(thread: &Value, mut stack_index: usize) -> Vec<String> {
    let mut functions = Vec::new();
    let mut visited = HashSet::new();

    while visited.insert(stack_index) {
        let Some(frame_index) = table_index(thread, "stackTable", "frame", stack_index) else {
            break;
        };
        let Some(function_index) = table_index(thread, "frameTable", "func", frame_index) else {
            break;
        };
        if let Some(function_name) = table_string(thread, "funcTable", "name", function_index) {
            functions.push(function_name.to_owned());
        }

        let Some(prefix_index) = table_index(thread, "stackTable", "prefix", stack_index) else {
            break;
        };
        stack_index = prefix_index;
    }

    functions
}

fn table_index(thread: &Value, table: &str, column: &str, row: usize) -> Option<usize> {
    thread
        .get(table)?
        .get(column)?
        .as_array()?
        .get(row)?
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
}

fn table_string<'a>(thread: &'a Value, table: &str, column: &str, row: usize) -> Option<&'a str> {
    let string_index = table_index(thread, table, column, row)?;
    thread.get("stringArray")?.as_array()?.get(string_index)?.as_str()
}

fn symbolize_cpu_profile_addresses<'a>(
    binary_path: &Path,
    functions: impl Iterator<Item = &'a String>,
) -> Value {
    let mut addresses =
        functions.filter_map(|function| parse_hex_address(function)).collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();

    if addresses.is_empty() {
        return json!({
            "status": "no_addresses",
            "symbols": {},
        });
    }

    if cfg!(target_os = "macos") {
        return symbolize_with_atos(binary_path, &addresses);
    }

    symbolize_with_addr2line(binary_path, &addresses)
}

fn parse_hex_address(function: &str) -> Option<u64> {
    function.strip_prefix("0x").and_then(|address| u64::from_str_radix(address, 16).ok())
}

fn symbolize_with_atos(binary_path: &Path, addresses: &[u64]) -> Value {
    let Some(load_address) = macho_text_vmaddr(binary_path) else {
        return json!({
            "status": "missing_load_address",
            "symbols": {},
        });
    };

    let mut command = Command::new("xcrun");
    command.arg("atos").arg("-o").arg(binary_path).arg("-l").arg(format!("0x{load_address:x}"));
    for address in addresses {
        command.arg(format!("0x{:x}", load_address + address));
    }

    let Ok(output) = command.output() else {
        return json!({
            "status": "failed_to_start",
            "symbols": {},
        });
    };
    if !output.status.success() {
        return json!({
            "status": "failed",
            "symbols": {},
            "stderr": String::from_utf8_lossy(&output.stderr).trim(),
        });
    }

    let symbols = String::from_utf8_lossy(&output.stdout)
        .lines()
        .zip(addresses.iter())
        .map(|(symbol, address)| (format!("0x{address:x}"), Value::String(symbol.to_owned())))
        .collect::<Map<_, _>>();

    json!({
        "status": "ok",
        "tool": "atos",
        "load_address": format!("0x{load_address:x}"),
        "symbols": symbols,
    })
}

fn macho_text_vmaddr(binary_path: &Path) -> Option<u64> {
    let output = Command::new("xcrun").arg("otool").arg("-l").arg(binary_path).output().ok()?;
    if !output.status.success() {
        return None;
    }

    let mut in_text_segment = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        if trimmed == "segname __TEXT" {
            in_text_segment = true;
            continue;
        }
        if in_text_segment && trimmed.starts_with("vmaddr ") {
            return trimmed
                .split_whitespace()
                .nth(1)
                .and_then(|address| address.strip_prefix("0x"))
                .and_then(|address| u64::from_str_radix(address, 16).ok());
        }
    }

    None
}

fn symbolize_with_addr2line(binary_path: &Path, addresses: &[u64]) -> Value {
    let mut command = Command::new("addr2line");
    command.arg("-e").arg(binary_path).arg("-f").arg("-C");
    for address in addresses {
        command.arg(format!("0x{address:x}"));
    }

    let Ok(output) = command.output() else {
        return json!({
            "status": "failed_to_start",
            "symbols": {},
        });
    };
    if !output.status.success() {
        return json!({
            "status": "failed",
            "symbols": {},
            "stderr": String::from_utf8_lossy(&output.stderr).trim(),
        });
    }

    let lines =
        String::from_utf8_lossy(&output.stdout).lines().map(ToOwned::to_owned).collect::<Vec<_>>();
    let mut symbols = Map::new();
    for (index, address) in addresses.iter().enumerate() {
        let function = lines.get(index * 2).cloned().unwrap_or_else(|| "??".to_owned());
        let location = lines.get(index * 2 + 1).cloned().unwrap_or_else(|| "??:0".to_owned());
        symbols.insert(format!("0x{address:x}"), Value::String(format!("{function} ({location})")));
    }

    json!({
        "status": "ok",
        "tool": "addr2line",
        "symbols": symbols,
    })
}

fn top_cpu_profile_entries(
    counts: &HashMap<String, u64>,
    symbols: Option<&Map<String, Value>>,
    limit: usize,
) -> Vec<Value> {
    top_cpu_profile_entries_with_filter(counts, symbols, limit, |_| true)
}

fn top_filtered_cpu_profile_entries(
    counts: &HashMap<String, u64>,
    symbols: Option<&Map<String, Value>>,
    limit: usize,
) -> Vec<Value> {
    top_cpu_profile_entries_with_filter(counts, symbols, limit, |function| {
        !is_filtered_cpu_profile_frame(function, symbols)
    })
}

fn top_etl_cpu_profile_entries(
    counts: &HashMap<String, u64>,
    symbols: Option<&Map<String, Value>>,
    limit: usize,
) -> Vec<Value> {
    top_cpu_profile_entries_with_filter(counts, symbols, limit, |function| {
        is_etl_cpu_profile_frame(function, symbols)
            && !is_filtered_cpu_profile_frame(function, symbols)
    })
}

fn top_cpu_profile_entries_with_filter(
    counts: &HashMap<String, u64>,
    symbols: Option<&Map<String, Value>>,
    limit: usize,
    keep_entry: impl Fn(&str) -> bool,
) -> Vec<Value> {
    let mut entries = counts.iter().collect::<Vec<_>>();
    entries.sort_by(|(left_function, left_count), (right_function, right_count)| {
        right_count.cmp(left_count).then_with(|| left_function.cmp(right_function))
    });

    entries
        .into_iter()
        .filter(|(function, _)| keep_entry(function))
        .take(limit)
        .map(|(function, count)| {
            let symbol = symbols
                .and_then(|symbols| symbols.get(function))
                .and_then(Value::as_str)
                .filter(|symbol| !symbol.starts_with("0x"));

            json!({
                "function": function,
                "symbol": symbol,
                "sample_count": count,
            })
        })
        .collect()
}

fn is_filtered_cpu_profile_frame(function: &str, symbols: Option<&Map<String, Value>>) -> bool {
    let symbol = symbols.and_then(|symbols| symbols.get(function)).and_then(Value::as_str);
    let normalized_function = function.to_ascii_lowercase();
    let normalized_symbol = symbol.map(str::to_ascii_lowercase);

    CPU_SAMPLE_SUMMARY_FILTER_PATTERNS.iter().any(|pattern| {
        normalized_function.contains(pattern)
            || normalized_symbol.as_deref().is_some_and(|symbol| symbol.contains(pattern))
    })
}

fn is_etl_cpu_profile_frame(function: &str, symbols: Option<&Map<String, Value>>) -> bool {
    let symbol = symbols.and_then(|symbols| symbols.get(function)).and_then(Value::as_str);
    let normalized_function = function.to_ascii_lowercase();
    let normalized_symbol = symbol.map(str::to_ascii_lowercase);

    CPU_SAMPLE_SUMMARY_ETL_PATTERNS.iter().any(|pattern| {
        normalized_function.contains(pattern)
            || normalized_symbol.as_deref().is_some_and(|symbol| symbol.contains(pattern))
    })
}

fn render_cpu_sample_summary_markdown(summary: &Value) -> String {
    let benchmark = summary.get("benchmark").and_then(Value::as_str).unwrap_or("unknown");
    let mut markdown = format!("# CPU Sample Summary: {benchmark}\n\n");

    if let Some(sample_count) = summary.get("sample_count").and_then(Value::as_u64) {
        markdown.push_str(&format!("- Samples: `{sample_count}`\n"));
    }
    if let Some(thread_count) = summary.get("thread_count").and_then(Value::as_u64) {
        markdown.push_str(&format!("- Threads: `{thread_count}`\n"));
    }
    if let Some(sample_count) = summary.get("application_sample_count").and_then(Value::as_u64) {
        markdown.push_str(&format!("- Application samples: `{sample_count}`\n"));
    }
    if let Some(thread_count) = summary.get("application_thread_count").and_then(Value::as_u64) {
        markdown.push_str(&format!("- Application threads: `{thread_count}`\n"));
    }
    if let Some(binary_path) = summary.get("binary_path").and_then(Value::as_str) {
        markdown.push_str(&format!("- Binary: `{binary_path}`\n"));
    }
    if let Some(status) = summary
        .get("symbolizer")
        .and_then(|symbolizer| symbolizer.get("status"))
        .and_then(Value::as_str)
    {
        markdown.push_str(&format!("- Symbolizer: `{status}`\n"));
    }
    if let Some(patterns) = summary
        .get("application_filter")
        .and_then(|filter| filter.get("hidden_patterns"))
        .and_then(Value::as_array)
    {
        let patterns = patterns.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("`, `");
        markdown.push_str(&format!("- Filtered application frames hide: `{patterns}`\n"));
    }
    if let Some(patterns) = summary
        .get("application_filter")
        .and_then(|filter| filter.get("etl_patterns"))
        .and_then(Value::as_array)
    {
        let patterns = patterns.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("`, `");
        markdown.push_str(&format!("- ETL-focused frames match: `{patterns}`\n"));
    }

    append_cpu_sample_table(
        &mut markdown,
        "Top ETL Application Inclusive Samples",
        summary.get("top_etl_application_inclusive"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top ETL Application Leaf Samples",
        summary.get("top_etl_application_leaf"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top Filtered Application Inclusive Samples",
        summary.get("top_filtered_application_inclusive"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top Filtered Application Leaf Samples",
        summary.get("top_filtered_application_leaf"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top Unfiltered Application Inclusive Samples",
        summary.get("top_application_inclusive"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top Unfiltered Application Leaf Samples",
        summary.get("top_application_leaf"),
    );
    append_cpu_sample_table(
        &mut markdown,
        "Top All-Thread Inclusive Samples",
        summary.get("top_inclusive"),
    );
    append_cpu_sample_table(&mut markdown, "Top All-Thread Leaf Samples", summary.get("top_leaf"));

    markdown
}

fn append_cpu_sample_table(markdown: &mut String, title: &str, rows: Option<&Value>) {
    markdown.push_str(&format!("\n## {title}\n\n"));
    markdown.push_str("| Samples | Function |\n");
    markdown.push_str("| ---: | --- |\n");

    let Some(rows) = rows.and_then(Value::as_array) else {
        markdown.push_str("_No samples matched this section._\n");
        return;
    };
    if rows.is_empty() {
        markdown.push_str("_No samples matched this section._\n");
        return;
    };
    for row in rows.iter().take(20) {
        let samples = row.get("sample_count").and_then(Value::as_u64).unwrap_or(0);
        let function = row
            .get("symbol")
            .and_then(Value::as_str)
            .or_else(|| row.get("function").and_then(Value::as_str))
            .unwrap_or("unknown");
        markdown.push_str(&format!("| {samples} | `{function}` |\n"));
    }
}

fn find_string_key(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(object) => {
            if let Some(found) = object.get(key).and_then(Value::as_str) {
                return Some(found.to_owned());
            }

            object.values().find_map(|value| find_string_key(value, key))
        }
        Value::Array(values) => values.iter().find_map(|value| find_string_key(value, key)),
        _ => None,
    }
}

fn render_artifact_markdown(summary: &Value) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Benchmark Artifacts\n\n");
    markdown.push_str(
        "This file indexes the benchmark and HotPath outputs for human review and LLM \
         analysis.\n\n",
    );

    if let Some(output_dir) = summary.get("output_dir").and_then(Value::as_str) {
        markdown.push_str(&format!("- Benchmark output directory: `{output_dir}`\n"));
    }
    if let Some(output_dir) = summary.get("hotpath_output_dir").and_then(Value::as_str) {
        markdown.push_str(&format!("- HotPath output directory: `{output_dir}`\n"));
    }
    if let Some(cpu_profile_copies) = summary.get("cpu_profile_copies").and_then(Value::as_array)
        && !cpu_profile_copies.is_empty()
    {
        markdown.push_str("- Copied CPU profiles:\n");
        for profile in cpu_profile_copies {
            let benchmark = profile.get("benchmark").and_then(Value::as_str).unwrap_or("unknown");
            let status = profile.get("status").and_then(Value::as_str).unwrap_or("unknown");
            let copied_path =
                profile.get("copied_path").and_then(Value::as_str).unwrap_or("not copied");
            markdown.push_str(&format!("  - `{benchmark}`: `{status}` `{copied_path}`\n"));
            if let Some(summary_path) = profile
                .get("sample_summary")
                .and_then(|summary| summary.get("markdown_path"))
                .and_then(Value::as_str)
            {
                markdown.push_str(&format!("    - sample summary: `{summary_path}`\n"));
            }
        }
    }
    markdown.push('\n');

    let Some(benchmarks) = summary.get("benchmarks").and_then(Value::as_array) else {
        return markdown;
    };

    for benchmark in benchmarks {
        let name = benchmark.get("benchmark").and_then(Value::as_str).unwrap_or("unknown");
        markdown.push_str(&format!("## {name}\n\n"));

        append_path(&mut markdown, "Benchmark JSON", benchmark.get("benchmark_report_path"));
        append_path(&mut markdown, "HotPath JSON", benchmark.get("hotpath_report_path"));
        if let Some(sample_paths) =
            benchmark.get("hotpath_sample_report_paths").and_then(Value::as_array)
            && !sample_paths.is_empty()
        {
            markdown.push_str("- HotPath sample JSON:\n");
            for path in sample_paths {
                if let Some(path) = path.as_str() {
                    markdown.push_str(&format!("  - `{path}`\n"));
                }
            }
        }

        let report = benchmark.get("benchmark_report").unwrap_or(&Value::Null);
        if report.get("status").and_then(Value::as_str) == Some("ok") {
            append_metric(&mut markdown, report, "rows_copied", "Rows copied");
            append_metric(&mut markdown, report, "rows_per_second", "Rows/s");
            append_metric(&mut markdown, report, "produced_events", "Produced events");
            append_metric(&mut markdown, report, "observed_cdc_events", "Observed CDC events");
            append_metric(&mut markdown, report, "events_per_second", "Events/s");
            append_metric(&mut markdown, report, "decoded_mib_per_second", "Decoded MiB/s");
            append_metric(&mut markdown, report, "total_ms", "Total ms");
            append_metric(&mut markdown, report, "null_flush_delay_min_ms", "Null flush min ms");
            append_metric(&mut markdown, report, "null_flush_delay_max_ms", "Null flush max ms");
        } else {
            markdown.push_str("- Status: not run\n");
        }

        if let Some(stats) = report.get("tokio_runtime_stats").filter(|value| !value.is_null()) {
            markdown.push_str(&format!("- Tokio runtime snapshot: `{}`\n", compact_json(stats)));
        }
        append_stage_breakdown(&mut markdown, report.get("stage_breakdown"));
        append_destination_distribution_summary(&mut markdown, report.get("destination_stats"));

        let hotpath = benchmark.get("hotpath_report").unwrap_or(&Value::Null);
        let flamegraph = hotpath.get("flamegraph").unwrap_or(&Value::Null);
        if let Some(status) = flamegraph.get("status").and_then(Value::as_str) {
            markdown.push_str(&format!("- Flamegraph/Samply status: `{status}`\n"));
        }
        if let Some(command) = flamegraph.get("open_command").and_then(Value::as_str) {
            markdown.push_str(&format!("- Open flamegraph/call tree: `{command}`\n"));
        }
        if let Some(message) = flamegraph.get("message").and_then(Value::as_str) {
            markdown.push_str(&format!("- CPU profiler message: `{message}`\n"));
        }

        markdown.push_str("\n### HotPath Sections\n\n");
        let Some(sections) = hotpath.get("sections").and_then(Value::as_object) else {
            markdown.push_str("HotPath report missing.\n\n");
            continue;
        };

        for section_name in [
            "functions_timing",
            "functions_alloc",
            "functions_cpu",
            "futures",
            "mutexes",
            "rw_locks",
            "sql",
            "threads",
            "cpu_baseline",
        ] {
            let Some(section) = sections.get(section_name) else {
                continue;
            };
            let count = section.get("count").and_then(Value::as_u64).unwrap_or(0);
            markdown.push_str(&format!("- `{section_name}`: {count} rows\n"));
            if let Some(message) = section.get("message").and_then(Value::as_str) {
                markdown.push_str(&format!("  - message: `{message}`\n"));
            }
            if let Some(top) = section.get("top").and_then(Value::as_array) {
                for row in top.iter().take(3) {
                    markdown.push_str(&format!("  - `{}`\n", compact_json(row)));
                }
            }
        }

        markdown.push('\n');
    }

    markdown
}

fn append_stage_breakdown(markdown: &mut String, stage_breakdown: Option<&Value>) {
    let Some(stages) = stage_breakdown.and_then(Value::as_object) else {
        return;
    };
    if stages.is_empty() {
        return;
    }

    markdown.push_str("- Stage breakdown:");
    for (stage, value) in stages {
        if let Some(value) = value.as_u64() {
            markdown.push_str(&format!(" `{stage}`={value}ms"));
        }
    }
    markdown.push('\n');
}

fn append_destination_distribution_summary(
    markdown: &mut String,
    destination_stats: Option<&Value>,
) {
    let Some(stats) = destination_stats.and_then(Value::as_object) else {
        return;
    };

    let fields = [
        ("table_row_batch_rows", "row batch rows"),
        ("table_row_batch_bytes", "row batch bytes"),
        ("table_row_write_ms", "row write ms"),
        ("event_batch_total_events", "event batch events"),
        ("event_batch_data_events", "event batch CDC events"),
        ("event_batch_bytes", "event batch bytes"),
        ("event_write_ms", "event write ms"),
    ];

    let mut wrote_header = false;
    for (field, label) in fields {
        let Some(summary) = stats.get(field).and_then(Value::as_object) else {
            continue;
        };
        let count = summary.get("count").and_then(Value::as_u64).unwrap_or(0);
        if count == 0 {
            continue;
        }
        if !wrote_header {
            markdown.push_str("- Destination distributions:\n");
            wrote_header = true;
        }

        let min = metric_summary_value(summary.get("min"));
        let avg = metric_summary_value(summary.get("avg"));
        let max = metric_summary_value(summary.get("max"));
        markdown.push_str(&format!("  - `{label}` count={count} min={min} avg={avg} max={max}\n"));
    }
}

fn metric_summary_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Null) | None => "n/a".to_owned(),
        Some(value) => compact_json(value),
    }
}

fn append_path(markdown: &mut String, label: &str, value: Option<&Value>) {
    if let Some(path) = value.and_then(Value::as_str) {
        markdown.push_str(&format!("- {label}: `{path}`\n"));
    }
}

fn append_metric(markdown: &mut String, report: &Value, key: &str, label: &str) {
    let Some(value) = report.get(key).filter(|value| !value.is_null()) else {
        return;
    };

    markdown.push_str(&format!("- {label}: `{}`\n", display_json_value(value)));
}

fn display_json_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        _ => compact_json(value),
    }
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

fn aggregate_reports(
    benchmark: &str,
    sample_count: usize,
    warmup_sample_count: usize,
    reports: &[Value],
) -> Result<Value> {
    if reports.is_empty() {
        bail!("cannot aggregate benchmark reports without measured samples");
    }

    let primary_metric = primary_metric_key(benchmark);
    let base_report = primary_metric
        .and_then(|key| median_report_by_metric(reports, key))
        .unwrap_or(&reports[reports.len() / 2]);
    let mut aggregate = aggregate_value(base_report, reports);
    let Some(object) = aggregate.as_object_mut() else {
        bail!("benchmark report is not a JSON object");
    };

    object.insert("sample_count".to_owned(), Value::Number(Number::from(sample_count as u64)));
    object.insert(
        "warmup_sample_count".to_owned(),
        Value::Number(Number::from(warmup_sample_count as u64)),
    );
    object.insert("aggregation".to_owned(), Value::String("median".to_owned()));
    object.insert("sample_summary".to_owned(), sample_summary(reports));
    Ok(aggregate)
}

fn aggregate_value(base: &Value, reports: &[Value]) -> Value {
    match base {
        Value::Object(base_object) => {
            let mut object = Map::new();
            for (key, value) in base_object {
                let values = reports
                    .iter()
                    .filter_map(|report| report.as_object()?.get(key))
                    .collect::<Vec<_>>();
                if values.len() == reports.len() {
                    object.insert(key.clone(), aggregate_field(value, &values));
                } else {
                    object.insert(key.clone(), value.clone());
                }
            }
            Value::Object(object)
        }
        value => value.clone(),
    }
}

fn aggregate_field(base: &Value, values: &[&Value]) -> Value {
    if let Some(numbers) = numeric_values(values) {
        return json_number(median(numbers));
    }

    if matches!(base, Value::Object(_)) && values.iter().all(|value| value.is_object()) {
        let owned_values = values.iter().map(|value| (*value).clone()).collect::<Vec<_>>();
        return aggregate_value(base, &owned_values);
    }

    base.clone()
}

fn sample_summary(reports: &[Value]) -> Value {
    let mut summary = Map::new();
    let Some(base_object) = reports.first().and_then(Value::as_object) else {
        return Value::Object(summary);
    };

    for key in base_object.keys() {
        let values =
            reports.iter().filter_map(|report| report.as_object()?.get(key)).collect::<Vec<_>>();
        let Some(numbers) = numeric_values(&values) else {
            continue;
        };
        if numbers.is_empty() {
            continue;
        }

        let min = numbers.iter().copied().fold(f64::INFINITY, f64::min);
        let max = numbers.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let median = median(numbers);
        let spread_pct = if min > 0.0 { ((max - min) / min) * 100.0 } else { 0.0 };
        summary.insert(
            key.clone(),
            Value::Object(Map::from_iter([
                ("min".to_owned(), json_number(min)),
                ("median".to_owned(), json_number(median)),
                ("max".to_owned(), json_number(max)),
                ("spread_pct".to_owned(), json_number(spread_pct)),
            ])),
        );
    }

    Value::Object(summary)
}

fn numeric_values(values: &[&Value]) -> Option<Vec<f64>> {
    values.iter().map(|value| value.as_f64()).collect()
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) { (values[mid - 1] + values[mid]) / 2.0 } else { values[mid] }
}

fn median_report_by_metric<'a>(reports: &'a [Value], key: &str) -> Option<&'a Value> {
    selected_report_index_by_metric(reports, key).map(|idx| &reports[idx])
}

fn selected_report_index(benchmark: &str, reports: &[Value]) -> Option<usize> {
    primary_metric_key(benchmark)
        .and_then(|key| selected_report_index_by_metric(reports, key))
        .or_else(|| (!reports.is_empty()).then_some(reports.len() / 2))
}

fn selected_report_index_by_metric(reports: &[Value], key: &str) -> Option<usize> {
    let mut indexed_values = reports
        .iter()
        .enumerate()
        .filter_map(|(idx, report)| Some((idx, report.get(key)?.as_f64()?)))
        .collect::<Vec<_>>();
    if indexed_values.len() != reports.len() {
        return None;
    }

    indexed_values.sort_by(|(_, left), (_, right)| left.total_cmp(right));
    Some(indexed_values[indexed_values.len() / 2].0)
}

fn json_number(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= u64::MAX as f64 {
        Value::Number(Number::from(value as u64))
    } else {
        Value::Number(Number::from_f64(value).unwrap_or_else(|| Number::from(0)))
    }
}

fn primary_metric_key(benchmark: &str) -> Option<&'static str> {
    match benchmark {
        "table_copy" => Some("rows_per_second"),
        "table_streaming" => Some("end_to_end_with_shutdown_events_per_second"),
        _ => None,
    }
}

fn print_sample_result(benchmark: &str, sample_label: &str, report: &Value) {
    match benchmark {
        "table_copy" => {
            let throughput = report_number(report, "rows_per_second").map_or_else(
                || "-".to_owned(),
                |value| format!("{} rows/s", format_decimal(value, 2)),
            );
            let total = report_number(report, "total_ms")
                .map_or_else(|| "-".to_owned(), format_milliseconds);
            print_done("Table copy", &format!("{sample_label} {throughput}, total {total}"));
        }
        "table_streaming" => {
            let throughput = report_number(report, "end_to_end_with_shutdown_events_per_second")
                .map_or_else(
                    || "-".to_owned(),
                    |value| format!("{} events/s", format_decimal(value, 2)),
                );
            let produced = report_number(report, "produced_events").map_or_else(
                || "-".to_owned(),
                |value| format!("{} events", format_integer(value as u64)),
            );
            let total = report_number(report, "total_ms")
                .map_or_else(|| "-".to_owned(), format_milliseconds);
            print_done(
                "Table streaming",
                &format!("{sample_label} {throughput}, {produced}, total {total}"),
            );
        }
        _ => {}
    }
}

fn print_configuration(rows: &[(&str, String)]) {
    print_stderr_table("Benchmark configuration", "Setting", "Value", rows);
    eprintln!();
}

fn print_phase(name: &str, message: &str) {
    let color = stderr_color_enabled();
    eprintln!(
        "{} {} {}",
        paint(color, "36;1", "[run]"),
        paint(color, "1", name),
        paint(color, "2", message)
    );
}

fn print_done(name: &str, message: &str) {
    let color = stderr_color_enabled();
    eprintln!("{} {} {}", paint(color, "32;1", "[ok] "), paint(color, "1", name), message);
}

fn print_skip(name: &str, message: &str) {
    let color = stderr_color_enabled();
    eprintln!("{} {} {}", paint(color, "33;1", "[skip]"), paint(color, "1", name), message);
}

fn print_stderr_table(title: &str, left_header: &str, right_header: &str, rows: &[(&str, String)]) {
    let color = stderr_color_enabled();
    eprintln!("{}", paint(color, "36;1", title));
    for line in table_lines(left_header, right_header, rows) {
        eprintln!("{line}");
    }
}

fn print_stdout_table(title: &str, left_header: &str, right_header: &str, rows: &[(&str, String)]) {
    let color = stdout_color_enabled();
    println!();
    println!("{}", paint(color, "36;1", title));
    for line in table_lines(left_header, right_header, rows) {
        println!("{line}");
    }
    println!();
}

fn table_lines(left_header: &str, right_header: &str, rows: &[(&str, String)]) -> Vec<String> {
    let left_width = rows
        .iter()
        .map(|(label, _)| label.len())
        .chain(std::iter::once(left_header.len()))
        .max()
        .unwrap_or(left_header.len());
    let right_width = rows
        .iter()
        .map(|(_, value)| value.len())
        .chain(std::iter::once(right_header.len()))
        .max()
        .unwrap_or(right_header.len());
    let border = format!(
        "+-{:-<left_width$}-+-{:-<right_width$}-+",
        "",
        "",
        left_width = left_width,
        right_width = right_width
    );

    let mut lines = Vec::with_capacity(rows.len() + 4);
    lines.push(border.clone());
    lines.push(format!("| {left_header:<left_width$} | {right_header:<right_width$} |"));
    lines.push(border.clone());
    for (label, value) in rows {
        lines.push(format!("| {label:<left_width$} | {value:<right_width$} |"));
    }
    lines.push(border);
    lines
}

fn stdout_color_enabled() -> bool {
    io::stdout().is_terminal() && color_allowed()
}

fn stderr_color_enabled() -> bool {
    io::stderr().is_terminal() && color_allowed()
}

fn color_allowed() -> bool {
    std::env::var_os("NO_COLOR").is_none()
        && std::env::var("TERM").map_or(true, |term| term != "dumb")
}

fn paint(enabled: bool, code: &str, text: &str) -> String {
    if enabled { format!("\x1b[{code}m{text}\x1b[0m") } else { text.to_owned() }
}

fn run_benchmark_binary(
    binary_name: &str,
    destination: Destination,
    benchmark_profile: &str,
    hotpath: HotpathRunConfig,
    binary_args: &[String],
    report_path: &Path,
) -> Result<Value> {
    let mut command = Command::new("cargo");
    command.args(["run", "--quiet", "-p", "etl-benchmarks", "--profile", benchmark_profile]);

    let features = cargo_features(destination, &hotpath);
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }

    if let Some(output_path) = &hotpath.output_path {
        command.env("HOTPATH_OUTPUT_FORMAT", "json");
        command.env("HOTPATH_OUTPUT_PATH", output_path);
    }
    if hotpath.mcp {
        command.env("HOTPATH_MCP_PORT", hotpath.mcp_port.to_string());
        command.env("HOTPATH_META_MCP_PORT", hotpath.mcp_port.to_string());
    }
    if hotpath.enabled {
        command.env("HOTPATH_FUNCTIONS_NAME_DEPTH", "0");
        command.env("HOTPATH_REPORT", "all");
    }
    if hotpath.alloc {
        command.env("HOTPATH_ALLOC_CUMULATIVE", "true");
    }

    command.args(["--bin", binary_name, "--"]).args(binary_args);

    print_phase(
        "Runner",
        &format!("{binary_name} benchmark running; detailed replicator logs are hidden"),
    );
    let output =
        command.output().with_context(|| format!("failed to run benchmark {binary_name}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        if !stdout.trim().is_empty() {
            eprintln!("{binary_name} stdout:");
            eprint!("{stdout}");
        }
        if !stderr.trim().is_empty() {
            eprintln!("{binary_name} stderr:");
            eprint!("{stderr}");
        }
        io::stderr().flush().context("failed to flush benchmark failure output")?;
        bail!("benchmark {binary_name} failed");
    }

    let report = read_report(report_path)
        .with_context(|| format!("benchmark {binary_name} did not write a valid report"))?;
    Ok(report)
}

fn cargo_features(destination: Destination, hotpath: &HotpathRunConfig) -> Vec<&'static str> {
    let mut features = Vec::new();
    if !matches!(destination, Destination::Null) {
        features.push(destination.as_arg());
    }
    if hotpath.enabled {
        features.push("hotpath");
    }
    if hotpath.alloc {
        features.push("hotpath-alloc");
    }
    if hotpath.cpu {
        features.push("hotpath-cpu");
    }
    if hotpath.mcp {
        features.push("hotpath-mcp");
    }

    features
}

fn read_report(path: &Path) -> Result<Value> {
    let json = fs::read_to_string(path)
        .with_context(|| format!("failed to read benchmark report {}", path.display()))?;
    serde_json::from_str(&json)
        .with_context(|| format!("failed to parse benchmark report {}", path.display()))
}

fn print_benchmark_report(binary_name: &str, report: &Value) {
    match binary_name {
        "table_copy" => {
            print_stdout_table(
                "Table copy results",
                "Metric",
                "Value",
                &report_rows(
                    report,
                    &[
                        ("Rows copied", "copied_rows"),
                        ("Rows/s", "rows_per_second"),
                        ("Decoded MiB", "estimated_copied_mib"),
                        ("Decoded MiB/s", "estimated_mib_per_second"),
                        ("Copy wait ms", "copy_wait_ms"),
                        ("Total ms", "total_ms"),
                    ],
                ),
            );
        }
        "table_streaming" => {
            print_stdout_table(
                "Table streaming results",
                "Metric",
                "Value",
                &report_rows(
                    report,
                    &[
                        ("Workload", "workload"),
                        ("Produced events", "produced_events"),
                        ("Observed CDC events", "observed_cdc_events"),
                        ("Events/s", "end_to_end_with_shutdown_events_per_second"),
                        ("Decoded MiB/s", "end_to_end_with_shutdown_estimated_mib_per_second"),
                        ("Total ms", "total_ms"),
                    ],
                ),
            );
        }
        _ => {}
    }
}

fn report_rows(
    report: &Value,
    metrics: &[(&'static str, &'static str)],
) -> Vec<(&'static str, String)> {
    metrics
        .iter()
        .filter_map(|(label, key)| {
            report.get(*key).map(|value| (*label, format_report_value(value)))
        })
        .collect()
}

fn print_combined_summary(table_copy: Option<&Value>, table_streaming: Option<&Value>) {
    let mut rows = Vec::new();
    if let Some(report) = table_copy {
        rows.push([
            "Table copy".to_owned(),
            report_number(report, "rows_per_second").map_or_else(
                || "-".to_owned(),
                |value| format!("{} rows/s", format_decimal(value, 2)),
            ),
            report_number(report, "estimated_mib_per_second").map_or_else(
                || "-".to_owned(),
                |value| format!("{} MiB/s", format_decimal(value, 2)),
            ),
            report_number(report, "total_ms").map_or_else(|| "-".to_owned(), format_milliseconds),
        ]);
    }
    if let Some(report) = table_streaming {
        rows.push([
            "Table streaming".to_owned(),
            report_number(report, "end_to_end_with_shutdown_events_per_second").map_or_else(
                || "-".to_owned(),
                |value| format!("{} events/s", format_decimal(value, 2)),
            ),
            report_number(report, "end_to_end_with_shutdown_estimated_mib_per_second").map_or_else(
                || "-".to_owned(),
                |value| format!("{} MiB/s", format_decimal(value, 2)),
            ),
            report_number(report, "total_ms").map_or_else(|| "-".to_owned(), format_milliseconds),
        ]);
    }

    if rows.is_empty() {
        return;
    }

    print_summary_table(&rows);
}

fn print_summary_table(rows: &[[String; 4]]) {
    let headers = ["Benchmark", "Throughput", "Data rate", "Total"];
    let mut widths = headers.map(str::len);
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }

    let border = format!(
        "+-{:-<w0$}-+-{:-<w1$}-+-{:-<w2$}-+-{:-<w3$}-+",
        "",
        "",
        "",
        "",
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3]
    );
    println!();
    println!("{}", paint(stdout_color_enabled(), "36;1", "Benchmark summary"));
    println!("{border}");
    println!(
        "| {:<w0$} | {:<w1$} | {:<w2$} | {:<w3$} |",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3]
    );
    println!("{border}");
    for row in rows {
        println!(
            "| {:<w0$} | {:<w1$} | {:<w2$} | {:<w3$} |",
            row[0],
            row[1],
            row[2],
            row[3],
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3]
        );
    }
    println!("{border}");
    println!();
}

fn format_report_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => match value.as_f64() {
            Some(value) if value.fract() == 0.0 && value >= 0.0 => format_integer(value as u64),
            Some(value) => format!("{value:.2}"),
            None => value.to_string(),
        },
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn report_number(report: &Value, key: &str) -> Option<f64> {
    report.get(key)?.as_f64()
}

fn format_milliseconds(value: f64) -> String {
    if value >= 1_000.0 {
        format!("{} s", format_decimal(value / 1_000.0, 2))
    } else {
        format!("{} ms", format_decimal(value, 0))
    }
}

fn format_decimal(value: f64, digits: usize) -> String {
    let raw = format!("{value:.digits$}");
    let Some((integer, fraction)) = raw.split_once('.') else {
        return format_integer_str(&raw);
    };
    format!("{}.{}", format_integer_str(integer), fraction)
}

fn format_integer_str(value: &str) -> String {
    let Some(unsigned) = value.strip_prefix('-') else {
        return format_integer(value.parse::<u64>().unwrap_or(0));
    };
    format!("-{}", format_integer(unsigned.parse::<u64>().unwrap_or(0)))
}

fn format_integer(value: u64) -> String {
    let value = value.to_string();
    let mut formatted = String::with_capacity(value.len() + value.len() / 3);
    let first_group_len = value.len() % 3;

    for (idx, ch) in value.chars().enumerate() {
        if idx > 0
            && (idx == first_group_len
                || (idx > first_group_len && (idx - first_group_len).is_multiple_of(3)))
        {
            formatted.push(',');
        }
        formatted.push(ch);
    }

    formatted
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::commands::benchmark::aggregate_reports;

    #[test]
    fn aggregate_reports_uses_medians_and_records_spread() {
        let reports = vec![
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "end_to_end_with_shutdown_events_per_second": 100.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 10.0,
                "total_ms": 1_000,
                "destination_stats": {
                    "inserts": 10,
                    "updates": 20
                }
            }),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "end_to_end_with_shutdown_events_per_second": 80.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 8.0,
                "total_ms": 1_200,
                "destination_stats": {
                    "inserts": 8,
                    "updates": 16
                }
            }),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "end_to_end_with_shutdown_events_per_second": 120.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 12.0,
                "total_ms": 900,
                "destination_stats": {
                    "inserts": 12,
                    "updates": 24
                }
            }),
        ];

        let aggregate = aggregate_reports("table_streaming", 3, 1, &reports).unwrap();

        assert_eq!(aggregate["aggregation"], "median");
        assert_eq!(aggregate["sample_count"], 3);
        assert_eq!(aggregate["warmup_sample_count"], 1);
        assert_eq!(aggregate["end_to_end_with_shutdown_events_per_second"], 100);
        assert_eq!(aggregate["end_to_end_with_shutdown_estimated_mib_per_second"], 10);
        assert_eq!(aggregate["total_ms"], 1000);
        assert_eq!(aggregate["destination_stats"]["inserts"], 10);
        assert_eq!(
            aggregate["sample_summary"]["end_to_end_with_shutdown_events_per_second"]["spread_pct"],
            50
        );
    }
}
