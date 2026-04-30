use std::{
    fs,
    io::{self, IsTerminal, Write},
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
        let table_copy_pipeline_id = pipeline_id_base;
        let table_streaming_pipeline_id =
            pipeline_id_base.checked_add(1).context("benchmark pipeline id overflow")?;
        let table_copy_publication_name = format!("bench_pub_{table_copy_pipeline_id}");
        let table_streaming_publication_name =
            format!("bench_streaming_pub_{table_streaming_pipeline_id}");

        print_configuration(&[
            ("Database", format!("{}@{}:{}", self.database, self.host, self.port)),
            ("Warehouses", self.warehouses.to_string()),
            ("TPC-C threads", tpcc_threads.to_string()),
            ("Streaming workload", "tpcc".to_owned()),
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
            self.create_publication(&table_copy_publication_name, &selected_tables)?;
            print_phase(
                "Table copy",
                &format!(
                    "copying {} rows from {} TPC-C tables",
                    format_integer(expected_row_count),
                    selected_tables.len()
                ),
            );

            table_copy_report = Some(self.run_table_copy(
                &table_copy_publication_name,
                &table_ids,
                expected_row_count,
                table_copy_pipeline_id,
                self.output_dir.join("table_copy.json"),
            )?);
            print_done("Table copy", "finished");
        } else {
            print_skip("Table copy", "disabled by flag");
        }

        if !self.skip_table_streaming {
            print_phase("Table streaming", "preparing benchmark");
            table_streaming_report = Some(self.run_table_streaming(
                &table_streaming_publication_name,
                tpcc_threads,
                table_streaming_pipeline_id,
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

    fn run_table_streaming(
        &self,
        publication_name: &str,
        tpcc_threads: u16,
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
            "--tpcc-warehouses".to_owned(),
            self.warehouses.to_string(),
            "--tpcc-threads".to_owned(),
            tpcc_threads.to_string(),
            "--drain-quiet-ms".to_owned(),
            self.streaming_drain_quiet_ms.to_string(),
        ]);

        let selected_tables = self.validated_tpcc_tables()?;
        let table_ids = self.fetch_table_ids(&selected_tables)?;
        self.create_publication(publication_name, &selected_tables)?;
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
    print_benchmark_report(binary_name, &report);
    print_done("Report", &format!("wrote {}", report_path.display()));
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
