use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use serde_json::Value;

const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_PORT: u16 = 5430;
const DEFAULT_DB_NAME: &str = "bench";
const DEFAULT_DB_USER: &str = "postgres";
const DEFAULT_DB_PASSWORD: &str = "postgres";
const DEFAULT_TPCC_TABLES: &str =
    "customer,district,item,new_order,order_line,orders,stock,warehouse";
const DEFAULT_PUBLICATION_NAME: &str = "bench_pub";
const DEFAULT_STREAMING_PUBLICATION_NAME: &str = "bench_streaming_pub";
const DEFAULT_STREAMING_TABLE_NAME: &str = "etl_streaming_benchmark";
const BENCHMARK_RESULT_PREFIX: &str = "BENCHMARK_RESULT ";
const DEFAULT_TPCC_THREADS_PER_WAREHOUSE: usize = 8;
const DEFAULT_TPCC_THREADS_PER_CPU: usize = 4;
const DEFAULT_TPCC_MIN_THREADS: usize = 8;
const DEFAULT_TPCC_MAX_THREADS: usize = 128;
const DEFAULT_STREAMING_MAX_PRODUCERS: usize = 64;

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
    /// BigQuery maximum staleness in minutes.
    #[arg(long, env = "BQ_MAX_STALENESS_MINS")]
    bq_max_staleness_mins: Option<u16>,
    /// BigQuery connection pool size.
    #[arg(long, env = "BQ_CONNECTION_POOL_SIZE", default_value_t = 32)]
    bq_connection_pool_size: usize,
    /// Table-copy benchmark publication name.
    #[arg(long, default_value = DEFAULT_PUBLICATION_NAME)]
    publication_name: String,
    /// Table-streaming benchmark publication name.
    #[arg(long, default_value = DEFAULT_STREAMING_PUBLICATION_NAME)]
    streaming_publication_name: String,
    /// Streaming benchmark table name.
    #[arg(long, default_value = DEFAULT_STREAMING_TABLE_NAME)]
    streaming_table_name: String,
    /// Number of insert events for table-streaming count mode.
    #[arg(long, default_value_t = 10_000)]
    streaming_events: u64,
    /// Run table-streaming producers for this many seconds instead of a fixed
    /// event count.
    #[arg(long)]
    streaming_duration_seconds: Option<u64>,
    /// Rows inserted per streaming producer transaction.
    #[arg(long, default_value_t = 5_000)]
    streaming_insert_batch_size: u64,
    /// Streaming producer concurrency. Defaults to a warehouse and CPU based
    /// value.
    #[arg(long)]
    streaming_producer_concurrency: Option<u16>,
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
    /// Disable ETL memory backpressure.
    #[arg(long, default_value_t = false)]
    disable_memory_backpressure: bool,
    /// Directory where JSON benchmark reports are written.
    #[arg(long, default_value = "target/bench-results")]
    output_dir: PathBuf,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Destination {
    Null,
    BigQuery,
}

impl BenchmarkArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.validate()?;
        check_command("psql")?;
        if !self.skip_prepare && !self.skip_table_copy {
            check_command("go-tpc")?;
        }

        fs::create_dir_all(&self.output_dir).context("failed to create benchmark output dir")?;

        let tpcc_threads =
            self.tpcc_threads.unwrap_or_else(|| recommended_threads(self.warehouses));
        let streaming_producer_concurrency = self
            .streaming_producer_concurrency
            .unwrap_or_else(|| recommended_streaming_producers(self.warehouses));
        let pipeline_id_base = benchmark_pipeline_id_base()?;
        let table_copy_pipeline_id = pipeline_id_base;
        let table_streaming_pipeline_id =
            pipeline_id_base.checked_add(1).context("benchmark pipeline id overflow")?;

        eprintln!("benchmark configuration:");
        eprintln!("  database: {}@{}:{}", self.database, self.host, self.port);
        eprintln!("  warehouses: {}", self.warehouses);
        eprintln!("  tpcc threads: {tpcc_threads}");
        eprintln!("  pipeline id base: {pipeline_id_base}");
        eprintln!("  destination: {}", self.destination.as_arg());
        eprintln!("  output dir: {}", self.output_dir.display());

        self.ensure_database()?;

        if !self.skip_table_copy {
            if !self.skip_prepare {
                self.prepare_tpcc(tpcc_threads)?;
            }

            let selected_tables = self.validated_tpcc_tables()?;
            let table_ids = self.fetch_table_ids(&selected_tables)?;
            let expected_row_count = self.fetch_expected_row_count(&selected_tables)?;
            self.create_publication(&self.publication_name, &selected_tables)?;

            let report =
                self.run_table_copy(&table_ids, expected_row_count, table_copy_pipeline_id)?;
            write_report(&self.output_dir, "table_copy", &report)?;
        }

        if !self.skip_table_streaming {
            let report = self
                .run_table_streaming(streaming_producer_concurrency, table_streaming_pipeline_id)?;
            write_report(&self.output_dir, "table_streaming", &report)?;
        }

        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.warehouses == 0 {
            bail!("--warehouses must be greater than 0");
        }

        if self.skip_table_copy && self.skip_table_streaming {
            bail!("at least one benchmark must run");
        }

        if matches!(self.destination, Destination::BigQuery) {
            if self.bq_project_id.as_deref().is_none_or(|project_id| project_id.trim().is_empty()) {
                bail!("--bq-project-id is required for --destination big-query");
            }

            if self.bq_dataset_id.as_deref().is_none_or(|dataset_id| dataset_id.trim().is_empty()) {
                bail!("--bq-dataset-id is required for --destination big-query");
            }

            let Some(sa_key_file) = &self.bq_sa_key_file else {
                bail!("--bq-sa-key-file is required for --destination big-query");
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
            eprintln!("TPC-C tables already exist; skipping preparation.");
            return Ok(());
        }

        eprintln!(
            "preparing TPC-C data with {} warehouses and {} threads.",
            self.warehouses, threads
        );

        let status = Command::new("go-tpc")
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
            .status()
            .context("failed to run go-tpc")?;

        if !status.success() {
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

    fn run_table_copy(
        &self,
        table_ids: &str,
        expected_row_count: u64,
        pipeline_id: u64,
    ) -> Result<Value> {
        let mut args = vec!["--log-target".to_owned(), "terminal".to_owned(), "run".to_owned()];
        self.push_common_benchmark_args(&mut args);
        self.push_tuning_args(&mut args);
        self.push_destination_args(&mut args);
        args.extend([
            "--pipeline-id".to_owned(),
            pipeline_id.to_string(),
            "--publication-name".to_owned(),
            self.publication_name.clone(),
            "--table-ids".to_owned(),
            table_ids.to_owned(),
            "--expected-row-count".to_owned(),
            expected_row_count.to_string(),
        ]);

        run_cargo_bench("table_copy", self.destination, &args)
    }

    fn run_table_streaming(&self, producer_concurrency: u16, pipeline_id: u64) -> Result<Value> {
        let mut args = vec!["--log-target".to_owned(), "terminal".to_owned(), "run".to_owned()];
        self.push_common_benchmark_args(&mut args);
        self.push_tuning_args(&mut args);
        self.push_destination_args(&mut args);
        args.extend([
            "--pipeline-id".to_owned(),
            pipeline_id.to_string(),
            "--publication-name".to_owned(),
            self.streaming_publication_name.clone(),
            "--table-name".to_owned(),
            self.streaming_table_name.clone(),
            "--event-count".to_owned(),
            self.streaming_events.to_string(),
            "--insert-batch-size".to_owned(),
            self.streaming_insert_batch_size.to_string(),
            "--producer-concurrency".to_owned(),
            producer_concurrency.to_string(),
        ]);

        if let Some(duration_seconds) = self.streaming_duration_seconds {
            args.extend(["--duration-seconds".to_owned(), duration_seconds.to_string()]);
        }

        run_cargo_bench("table_streaming", self.destination, &args)
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

        if self.disable_memory_backpressure {
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

        if let Some(max_staleness_mins) = self.bq_max_staleness_mins {
            args.extend(["--bq-max-staleness-mins".to_owned(), max_staleness_mins.to_string()]);
        }

        args.extend([
            "--bq-connection-pool-size".to_owned(),
            self.bq_connection_pool_size.to_string(),
        ]);
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
}

impl Destination {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::BigQuery => "big-query",
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

fn recommended_streaming_producers(warehouses: u16) -> u16 {
    let cpu_producers =
        std::thread::available_parallelism().map(std::num::NonZero::get).unwrap_or(1);
    usize::from(warehouses).max(cpu_producers).clamp(1, DEFAULT_STREAMING_MAX_PRODUCERS) as u16
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

fn run_cargo_bench(
    bench_name: &str,
    destination: Destination,
    bench_args: &[String],
) -> Result<Value> {
    let mut command = Command::new("cargo");
    command.args(["bench", "-p", "etl-benchmarks"]);
    if matches!(destination, Destination::BigQuery) {
        command.args(["--features", "bigquery"]);
    }
    command.args(["--bench", bench_name, "--"]).args(bench_args);

    eprintln!("running cargo bench -p etl-benchmarks --bench {bench_name}.");
    let output =
        command.output().with_context(|| format!("failed to run benchmark {bench_name}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprint!("{stderr}");
    io::stderr().flush().context("failed to flush benchmark stderr")?;
    print!("{stdout}");
    io::stdout().flush().context("failed to flush benchmark stdout")?;

    if !output.status.success() {
        bail!("benchmark {bench_name} failed");
    }

    parse_benchmark_result(&stdout)
        .with_context(|| format!("benchmark {bench_name} did not print a result JSON line"))
}

fn parse_benchmark_result(stdout: &str) -> Option<Value> {
    stdout.lines().rev().find_map(|line| {
        let json = line.strip_prefix(BENCHMARK_RESULT_PREFIX)?;
        serde_json::from_str(json).ok()
    })
}

fn write_report(output_dir: &Path, name: &str, report: &Value) -> Result<()> {
    let path = output_dir.join(format!("{name}.json"));
    let json = serde_json::to_string_pretty(report)?;
    fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("failed to write benchmark report to {}", path.display()))?;
    eprintln!("wrote {}", path.display());
    Ok(())
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
