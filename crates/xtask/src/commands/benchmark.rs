use std::{
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use serde_json::{Map, Number, Value};

const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_PORT: u16 = 5430;
const DEFAULT_DB_NAME: &str = "bench";
const DEFAULT_DB_USER: &str = "postgres";
const DEFAULT_DB_PASSWORD: &str = "postgres";
const DEFAULT_TPCC_TABLES: &str =
    "customer,district,item,new_order,order_line,orders,stock,warehouse";
const DEFAULT_STREAMING_DURATION_SECONDS: u64 = 60;
const DEFAULT_TPCC_THREADS_PER_WAREHOUSE: usize = 8;
const DEFAULT_TPCC_THREADS_PER_CPU: usize = 2;
const DEFAULT_TPCC_MIN_THREADS: usize = 8;
const DEFAULT_TPCC_MAX_THREADS: usize = 64;
const MEMORY_BACKPRESSURE_ACTIVATE_THRESHOLD: f32 = 0.85;
const MEMORY_BACKPRESSURE_RESUME_THRESHOLD: f32 = 0.75;

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
    #[arg(long, env = "TPCC_WAREHOUSES", default_value_t = 1)]
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
    /// Run the TPC-C streaming workload for this many seconds.
    #[arg(long)]
    streaming_duration_seconds: Option<u64>,
    /// Quiet period with no CDC events before TPC-C streaming is considered
    /// drained.
    #[arg(long, default_value_t = 2_000)]
    streaming_drain_quiet_ms: u64,
    /// Maximum batch fill time in milliseconds.
    #[arg(long, default_value_t = 1_000)]
    batch_max_fill_ms: u64,
    /// Ratio of process memory reserved for stream batch bytes.
    #[arg(long, default_value_t = 0.2)]
    memory_budget_ratio: f32,
    /// Maximum table sync workers.
    #[arg(long, default_value_t = 4)]
    max_table_sync_workers: u16,
    /// Maximum parallel copy connections per table.
    #[arg(long, default_value_t = 2)]
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
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Destination {
    Null,
    #[value(name = "bigquery")]
    BigQuery,
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

        let tpcc_threads =
            self.tpcc_threads.unwrap_or_else(|| recommended_threads(self.warehouses));
        let pipeline_id_base = benchmark_pipeline_id_base()?;

        print_configuration(&[
            ("Database", format!("{}@{}:{}", self.database, self.host, self.port)),
            ("Warehouses", self.warehouses.to_string()),
            ("TPC-C threads", tpcc_threads.to_string()),
            ("Streaming workload", "tpcc".to_owned()),
            ("Measured samples", self.samples.to_string()),
            ("Warmup samples", self.warmup_samples.to_string()),
            ("Pipeline id base", pipeline_id_base.to_string()),
            ("Destination", self.destination.as_arg().to_owned()),
            ("Memory backpressure", memory_backpressure_label(self.enable_memory_backpressure)),
            ("Memory budget ratio", format_decimal(f64::from(self.memory_budget_ratio), 2)),
            ("Output dir", self.output_dir.display().to_string()),
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

        if let Some(duration_seconds) = self.streaming_duration_seconds
            && duration_seconds == 0
        {
            bail!("--streaming-duration-seconds must be greater than 0");
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
            &format!("create database {}", quote_identifier(&self.database)?),
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
            .map(|table| Ok(format!("(select count(*) from {})", quote_identifier(table)?)))
            .collect::<Result<Vec<_>>>()?
            .join(" + ");
        let count = self.psql_query(&self.database, &format!("select {query}"))?;

        Ok(count.trim().parse()?)
    }

    fn create_publication(&self, publication_name: &str, tables: &[String]) -> Result<()> {
        let publication_name = quote_identifier(publication_name)?;
        let table_list = tables
            .iter()
            .map(|table| quote_identifier(table))
            .collect::<Result<Vec<_>>>()?
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
        let publication_name = quote_identifier(publication_name)?;
        self.psql_status(&self.database, &format!("drop publication if exists {publication_name}"))
            .context("failed to drop benchmark publication")
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
        let total_samples = self.total_sample_count();
        for sample_idx in 0..total_samples {
            let pipeline_id = sample_pipeline_id(pipeline_id_base, 0, sample_idx)?;
            let publication_name = format!("bench_pub_{pipeline_id}");
            let sample_report_path =
                self.sample_report_path("table_copy", sample_idx.saturating_add(1));
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
            )?;
            self.drop_publication(&publication_name)?;
            remove_sample_report(&sample_report_path)?;
            print_sample_result("table_copy", &sample_label, &report);

            if self.is_warmup_sample(sample_idx) {
                print_skip("Table copy", &format!("{sample_label} discarded as warmup"));
            } else {
                measured_reports.push(report);
            }
        }

        let aggregate = aggregate_reports(
            "table_copy",
            usize::from(self.samples),
            usize::from(self.warmup_samples),
            &measured_reports,
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
        self.push_destination_args(&mut args);
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

        run_benchmark_binary("table_copy", self.destination, &args, &report_path)
    }

    fn run_table_streaming_samples(
        &self,
        tpcc_threads: u16,
        pipeline_id_base: u64,
        report_path: PathBuf,
    ) -> Result<Value> {
        let selected_tables = self.validated_tpcc_tables()?;
        let mut measured_reports = Vec::with_capacity(usize::from(self.samples));
        let total_samples = self.total_sample_count();
        for sample_idx in 0..total_samples {
            let pipeline_id = sample_pipeline_id(pipeline_id_base, 1, sample_idx)?;
            let publication_name = format!("bench_streaming_pub_{pipeline_id}");
            let sample_report_path =
                self.sample_report_path("table_streaming", sample_idx.saturating_add(1));
            let sample_label = self.sample_label(sample_idx);

            print_phase("Table streaming", &format!("{sample_label} preparing"));
            let report = self.run_table_streaming(
                &publication_name,
                tpcc_threads,
                pipeline_id,
                &selected_tables,
                sample_report_path.clone(),
            )?;
            self.drop_publication(&publication_name)?;
            remove_sample_report(&sample_report_path)?;
            print_sample_result("table_streaming", &sample_label, &report);

            if self.is_warmup_sample(sample_idx) {
                print_skip("Table streaming", &format!("{sample_label} discarded as warmup"));
            } else {
                measured_reports.push(report);
            }
        }

        let aggregate = aggregate_reports(
            "table_streaming",
            usize::from(self.samples),
            usize::from(self.warmup_samples),
            &measured_reports,
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
        self.push_destination_args(&mut args);
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

        run_benchmark_binary("table_streaming", self.destination, &args, &report_path)
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

    fn push_destination_args(&self, args: &mut Vec<String>) {
        args.extend(["--destination".to_owned(), self.destination.as_arg().to_owned()]);

        if !matches!(self.destination, Destination::BigQuery) {
            return;
        }

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
}

impl Destination {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::BigQuery => "bigquery",
        }
    }
}

fn recommended_threads(warehouses: u16) -> u16 {
    let warehouse_threads =
        usize::from(warehouses).saturating_mul(DEFAULT_TPCC_THREADS_PER_WAREHOUSE);
    let cpu_threads = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().saturating_mul(DEFAULT_TPCC_THREADS_PER_CPU))
        .unwrap_or(DEFAULT_TPCC_MIN_THREADS);
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
    let mut indexed_values = reports
        .iter()
        .enumerate()
        .filter_map(|(idx, report)| Some((idx, report.get(key)?.as_f64()?)))
        .collect::<Vec<_>>();
    if indexed_values.len() != reports.len() {
        return None;
    }

    indexed_values.sort_by(|(_, left), (_, right)| left.total_cmp(right));
    Some(&reports[indexed_values[indexed_values.len() / 2].0])
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
    binary_args: &[String],
    report_path: &Path,
) -> Result<Value> {
    let mut command = Command::new("cargo");
    command.args(["run", "--quiet", "-p", "etl-benchmarks", "--release"]);
    if matches!(destination, Destination::BigQuery) {
        command.args(["--features", "bigquery"]);
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

fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier.is_empty() {
        bail!("identifier cannot be empty");
    }

    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
