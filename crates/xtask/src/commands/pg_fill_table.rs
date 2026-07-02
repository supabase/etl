use std::{
    io::Write,
    process::{Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use pg_escape::quote_identifier;

const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_NAME: &str = "bench";
const DEFAULT_DB_USER: &str = "postgres";
const DEFAULT_DB_PASSWORD: &str = "postgres";
const DEFAULT_CONN_PARAMS: &str = "sslmode=require";
const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_PARALLELISM: u16 = 8;
const DEFAULT_ROW_BYTES: usize = 16 * 1024;
const DEFAULT_ROWS_PER_COPY: u32 = 4_096;
const DEFAULT_MONITOR_SECONDS: u64 = 10;
const DEFAULT_STALL_SECONDS: u64 = 180;
const BULK_LOAD_PGOPTIONS: &str = "-c synchronous_commit=off -c statement_timeout=0 -c \
                                   lock_timeout=0 -c idle_in_transaction_session_timeout=0 -c \
                                   client_min_messages=warning";

#[derive(Args)]
pub(crate) struct PgFillTableArgs {
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
    /// Extra Postgres connection parameters passed to psql.
    #[arg(long, default_value = DEFAULT_CONN_PARAMS)]
    conn_params: String,
    /// Schema that owns the fill table.
    #[arg(long, default_value = DEFAULT_SCHEMA)]
    schema: String,
    /// Fill table name.
    #[arg(long)]
    table: String,
    /// Target committed payload size for the table, for example `450gb`.
    #[arg(long)]
    target_size: String,
    /// Number of parallel COPY workers.
    #[arg(long, default_value_t = DEFAULT_PARALLELISM)]
    parallelism: u16,
    /// Approximate generated payload bytes per row.
    #[arg(long, default_value_t = DEFAULT_ROW_BYTES)]
    row_bytes: usize,
    /// Rows written by each worker per COPY transaction.
    #[arg(long, default_value_t = DEFAULT_ROWS_PER_COPY)]
    rows_per_copy: u32,
    /// Seconds between table-size checks.
    #[arg(long, default_value_t = DEFAULT_MONITOR_SECONDS)]
    monitor_seconds: u64,
    /// Maximum seconds to run. Use 0 for no time limit.
    #[arg(long, default_value_t = 0)]
    max_seconds: u64,
    /// Maximum seconds with no COPY send or commit progress. Use 0 to disable.
    #[arg(long, default_value_t = DEFAULT_STALL_SECONDS)]
    stall_seconds: u64,
    /// Use an UNLOGGED table. This is faster but cannot be logically
    /// replicated.
    #[arg(long, default_value_t = false)]
    unlogged: bool,
    /// Drop the target table before loading.
    #[arg(long, default_value_t = false)]
    force: bool,
}

impl PgFillTableArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.validate()?;
        check_command("psql")?;

        let target_bytes = parse_size(&self.target_size)?;
        let table = QualifiedTable::new(&self.schema, &self.table)?;
        self.prepare_table(&table)?;

        let initial_size = self.table_size_bytes(&table)?;
        print_configuration(&[
            ("Database", format!("{}@{}:{}", self.database, self.host, self.port)),
            ("Table", table.display()),
            ("Target committed payload", format_bytes(target_bytes)),
            ("Current table size", format_bytes(initial_size)),
            ("Parallelism", self.parallelism.to_string()),
            ("Row bytes", self.row_bytes.to_string()),
            ("Rows per COPY", self.rows_per_copy.to_string()),
            ("Stall seconds", stall_seconds_label(self.stall_seconds)),
            ("Table persistence", if self.unlogged { "unlogged" } else { "logged" }.to_owned()),
            ("Connection params", self.conn_params.clone()),
            ("Session options", BULK_LOAD_PGOPTIONS.to_owned()),
        ]);

        let stop = Arc::new(AtomicBool::new(false));
        let rows_sent = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));
        let rows_committed = Arc::new(AtomicU64::new(0));
        let bytes_committed = Arc::new(AtomicU64::new(0));
        let (worker_error_tx, worker_error_rx) = mpsc::channel();
        let mut handles = Vec::with_capacity(usize::from(self.parallelism));

        for worker_id in 0..self.parallelism {
            let worker = FillWorker {
                args: self.clone_for_worker(),
                table: table.clone(),
                worker_id,
                payload: make_payload(self.row_bytes, u64::from(worker_id)),
                stop: Arc::clone(&stop),
                rows_sent: Arc::clone(&rows_sent),
                bytes_sent: Arc::clone(&bytes_sent),
                rows_committed: Arc::clone(&rows_committed),
                bytes_committed: Arc::clone(&bytes_committed),
            };
            let worker_error_tx = worker_error_tx.clone();
            handles.push(thread::spawn(move || {
                let result = worker.run();
                if let Err(error) = &result {
                    let _ = worker_error_tx.send(format!("{error:#}"));
                }
                result
            }));
        }
        drop(worker_error_tx);

        let start = Instant::now();
        let mut last_activity_at = start;
        let mut last_sent_bytes = 0;
        let mut last_committed_bytes = 0;
        let monitor_interval = Duration::from_secs(self.monitor_seconds);
        let mut timeout_error = None;
        loop {
            thread::sleep(monitor_interval);

            if let Ok(error) = worker_error_rx.try_recv() {
                stop.store(true, Ordering::Relaxed);
                timeout_error = Some(format!("COPY worker failed: {error}"));
                break;
            }

            let size_bytes = self.table_size_bytes(&table)?;
            let elapsed = start.elapsed().as_secs_f64();
            let sent_bytes = bytes_sent.load(Ordering::Relaxed);
            let committed_bytes = bytes_committed.load(Ordering::Relaxed);
            let sent_mib_per_second = sent_bytes as f64 / elapsed / 1024.0 / 1024.0;
            let committed_mib_per_second = committed_bytes as f64 / elapsed / 1024.0 / 1024.0;
            println!(
                "table_size={} rows_sent={} payload_sent={} send_rate={:.2} MiB/s \
                 rows_committed={} payload_committed={} commit_rate={:.2} MiB/s",
                format_bytes(size_bytes),
                rows_sent.load(Ordering::Relaxed),
                format_bytes(sent_bytes),
                sent_mib_per_second,
                rows_committed.load(Ordering::Relaxed),
                format_bytes(committed_bytes),
                committed_mib_per_second,
            );

            if sent_bytes > last_sent_bytes || committed_bytes > last_committed_bytes {
                last_sent_bytes = sent_bytes;
                last_committed_bytes = committed_bytes;
                last_activity_at = Instant::now();
            }

            if committed_bytes >= target_bytes {
                stop.store(true, Ordering::Relaxed);
                break;
            }

            if self.stall_seconds != 0
                && last_activity_at.elapsed() >= Duration::from_secs(self.stall_seconds)
            {
                stop.store(true, Ordering::Relaxed);
                timeout_error = Some(format!(
                    "no COPY progress for {} seconds; sent payload was {}, committed payload was \
                     {}, physical table size was {}",
                    self.stall_seconds,
                    format_bytes(sent_bytes),
                    format_bytes(committed_bytes),
                    format_bytes(size_bytes)
                ));
                break;
            }

            if self.max_seconds != 0 && start.elapsed() >= Duration::from_secs(self.max_seconds) {
                stop.store(true, Ordering::Relaxed);
                timeout_error = Some(format!(
                    "target was not reached within {} seconds; last table size was {}",
                    self.max_seconds,
                    format_bytes(size_bytes)
                ));
                break;
            }
        }

        for handle in handles {
            handle.join().expect("COPY worker thread should not panic")?;
        }

        if let Some(error) = timeout_error {
            bail!("{error}");
        }

        let final_size = self.table_size_bytes(&table)?;
        println!(
            "target reached; final table size={}, committed payload={}",
            format_bytes(final_size),
            format_bytes(bytes_committed.load(Ordering::Relaxed))
        );
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.parallelism == 0 {
            bail!("--parallelism must be greater than 0");
        }

        if self.row_bytes == 0 {
            bail!("--row-bytes must be greater than 0");
        }

        if self.rows_per_copy == 0 {
            bail!("--rows-per-copy must be greater than 0");
        }

        if self.monitor_seconds == 0 {
            bail!("--monitor-seconds must be greater than 0");
        }

        Ok(())
    }

    fn clone_for_worker(&self) -> WorkerArgs {
        WorkerArgs {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            conn_params: self.conn_params.clone(),
            rows_per_copy: self.rows_per_copy,
        }
    }

    fn prepare_table(&self, table: &QualifiedTable) -> Result<()> {
        let persistence = if self.unlogged { "unlogged " } else { "" };
        let drop_sql = if self.force {
            format!("drop table if exists {} cascade;", table.quoted())
        } else {
            String::new()
        };
        let sql = format!(
            "{drop_sql}create schema if not exists {}; create {persistence}table if not exists {} \
             (id bigint generated by default as identity primary key, worker_id integer not null, \
             row_id bigint not null, chunk_id bigint not null, payload text not null, inserted_at \
             timestamptz not null default now()) with (fillfactor = 100, autovacuum_enabled = \
             false); alter table {} alter column payload set storage external;",
            quote_identifier(&table.schema),
            table.quoted(),
            table.quoted(),
        );
        self.psql_status(&sql).context("failed to prepare fill table")
    }

    fn table_size_bytes(&self, table: &QualifiedTable) -> Result<u64> {
        let sql = format!(
            "select coalesce(pg_total_relation_size(to_regclass({})), 0)",
            quote_literal(&table.display())
        );
        let output = self.psql_query(&sql)?;
        output.trim().parse().context("failed to parse table size")
    }

    fn psql_query(&self, sql: &str) -> Result<String> {
        let output = self.psql_output(sql)?;
        if !output.status.success() {
            bail!("psql query failed: {}", String::from_utf8_lossy(&output.stderr).trim());
        }

        String::from_utf8(output.stdout).context("psql output was not valid UTF-8")
    }

    fn psql_status(&self, sql: &str) -> Result<()> {
        let output = self.psql_output(sql)?;
        if !output.status.success() {
            bail!("psql command failed: {}", String::from_utf8_lossy(&output.stderr).trim());
        }

        Ok(())
    }

    fn psql_output(&self, sql: &str) -> Result<Output> {
        let mut command = base_psql_command(
            &self.host,
            self.port,
            &self.database,
            &self.username,
            &self.password,
            &self.conn_params,
        );
        command.args(["-tAc", sql]);
        command.output().context("failed to run psql")
    }
}

#[derive(Clone)]
struct QualifiedTable {
    schema: String,
    table: String,
}

impl QualifiedTable {
    fn new(schema: &str, table: &str) -> Result<Self> {
        validate_identifier(schema, "schema")?;
        validate_identifier(table, "table")?;
        Ok(Self { schema: schema.to_owned(), table: table.to_owned() })
    }

    fn quoted(&self) -> String {
        format!("{}.{}", quote_identifier(&self.schema), quote_identifier(&self.table))
    }

    fn display(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

struct WorkerArgs {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    conn_params: String,
    rows_per_copy: u32,
}

struct FillWorker {
    args: WorkerArgs,
    table: QualifiedTable,
    worker_id: u16,
    payload: String,
    stop: Arc<AtomicBool>,
    rows_sent: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
    rows_committed: Arc<AtomicU64>,
    bytes_committed: Arc<AtomicU64>,
}

impl FillWorker {
    fn run(self) -> Result<()> {
        let mut row_id = 0_u64;
        let mut chunk_id = 0_u64;
        while !self.stop.load(Ordering::Relaxed) {
            let copied_rows = self.copy_chunk(row_id, chunk_id)?;
            if copied_rows == 0 {
                break;
            }

            self.add_committed_rows(copied_rows);
            row_id = row_id.saturating_add(copied_rows);
            chunk_id = chunk_id.saturating_add(1);
        }

        Ok(())
    }

    fn copy_chunk(&self, first_row_id: u64, chunk_id: u64) -> Result<u64> {
        let sql = format!(
            "copy {} (worker_id, row_id, chunk_id, payload) from stdin with (format csv)",
            self.table.quoted()
        );
        let mut child = base_psql_command(
            &self.args.host,
            self.args.port,
            &self.args.database,
            &self.args.username,
            &self.args.password,
            &self.args.conn_params,
        )
        .args(["-c", &sql])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to start psql COPY worker")?;

        let mut copied_rows = 0_u64;
        {
            let stdin = child.stdin.as_mut().context("psql stdin was not piped")?;
            let mut pending_counter_rows = 0_u64;
            for offset in 0..self.args.rows_per_copy {
                if self.stop.load(Ordering::Relaxed) {
                    break;
                }

                let row_id = first_row_id + u64::from(offset);
                writeln!(stdin, "{},{},{},{}", self.worker_id, row_id, chunk_id, self.payload)
                    .context("failed to write COPY row")?;
                copied_rows = copied_rows.saturating_add(1);
                pending_counter_rows = pending_counter_rows.saturating_add(1);

                if pending_counter_rows >= 128 {
                    self.add_sent_rows(pending_counter_rows);
                    pending_counter_rows = 0;
                }
            }

            if pending_counter_rows > 0 {
                self.add_sent_rows(pending_counter_rows);
            }

            stdin.write_all(b"\\.\n").context("failed to finish COPY stream")?;
        }

        let output = child.wait_with_output().context("failed to wait for psql COPY worker")?;
        if !output.status.success() {
            bail!("psql COPY failed: {}", String::from_utf8_lossy(&output.stderr).trim());
        }

        Ok(copied_rows)
    }

    fn add_sent_rows(&self, rows: u64) {
        self.rows_sent.fetch_add(rows, Ordering::Relaxed);
        self.bytes_sent
            .fetch_add(rows.saturating_mul(self.payload.len() as u64), Ordering::Relaxed);
    }

    fn add_committed_rows(&self, rows: u64) {
        self.rows_committed.fetch_add(rows, Ordering::Relaxed);
        self.bytes_committed
            .fetch_add(rows.saturating_mul(self.payload.len() as u64), Ordering::Relaxed);
    }
}

fn base_psql_command(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    conn_params: &str,
) -> Command {
    let mut command = Command::new("psql");
    command
        .args(["-v", "ON_ERROR_STOP=1"])
        .args(["-h", host])
        .args(["-U", username])
        .args(["-p", &port.to_string()])
        .args(["-d", database])
        .env("PGPASSWORD", password)
        .env("PGOPTIONS", BULK_LOAD_PGOPTIONS);

    apply_psql_conn_params(&mut command, conn_params);
    command
}

fn parse_size(input: &str) -> Result<u64> {
    let input = input.trim();
    if input.is_empty() {
        bail!("size must not be empty");
    }

    let number_end =
        input.find(|ch: char| !(ch.is_ascii_digit() || ch == '.')).unwrap_or(input.len());
    let (number, unit) = input.split_at(number_end);
    let value: f64 = number.parse().with_context(|| format!("invalid size '{input}'"))?;
    if !value.is_finite() || value <= 0.0 {
        bail!("size must be greater than 0");
    }

    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1_f64,
        "kb" => 1_000_f64,
        "mb" => 1_000_000_f64,
        "gb" => 1_000_000_000_f64,
        "tb" => 1_000_000_000_000_f64,
        "kib" => 1_024_f64,
        "mib" => 1_024_f64.powi(2),
        "gib" => 1_024_f64.powi(3),
        "tib" => 1_024_f64.powi(4),
        _ => bail!("unsupported size unit '{unit}'"),
    };

    let bytes = value * multiplier;
    if bytes > u64::MAX as f64 {
        bail!("size is too large");
    }

    Ok(bytes.ceil() as u64)
}

fn stall_seconds_label(stall_seconds: u64) -> String {
    if stall_seconds == 0 { "disabled".to_owned() } else { stall_seconds.to_string() }
}

fn make_payload(bytes: usize, seed: u64) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut payload = Vec::with_capacity(bytes);
    for _ in 0..bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(ALPHABET[state as usize % ALPHABET.len()]);
    }

    String::from_utf8(payload).expect("payload alphabet should be valid UTF-8")
}

fn validate_identifier(identifier: &str, label: &str) -> Result<()> {
    if identifier.trim().is_empty() {
        bail!("{label} must not be empty");
    }

    if identifier.bytes().any(|byte| byte == 0) {
        bail!("{label} must not contain NUL bytes");
    }

    Ok(())
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn apply_psql_conn_params(command: &mut Command, conn_params: &str) {
    for part in conn_params.split_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };

        match key {
            "sslmode" => {
                command.env("PGSSLMODE", value);
            }
            "sslrootcert" => {
                command.env("PGSSLROOTCERT", value);
            }
            "sslcert" => {
                command.env("PGSSLCERT", value);
            }
            "sslkey" => {
                command.env("PGSSLKEY", value);
            }
            "connect_timeout" => {
                command.env("PGCONNECT_TIMEOUT", value);
            }
            _ => {}
        }
    }
}

fn check_command(command: &str) -> Result<()> {
    let status =
        Command::new(command).arg("--version").stdout(Stdio::null()).stderr(Stdio::null()).status();

    if status.is_ok_and(|status| status.success()) {
        return Ok(());
    }

    bail!("required command '{command}' was not found")
}

fn print_configuration(rows: &[(&str, String)]) {
    println!("Postgres table fill configuration");
    for (key, value) in rows {
        println!("  {key:<26} {value}");
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 { format!("{bytes} B") } else { format!("{value:.2} {}", UNITS[unit]) }
}

#[cfg(test)]
mod tests {
    use super::{QualifiedTable, make_payload, parse_size};

    #[test]
    fn parses_decimal_and_binary_sizes() {
        assert_eq!(parse_size("450gb").unwrap(), 450_000_000_000);
        assert_eq!(parse_size("1GiB").unwrap(), 1_073_741_824);
    }

    #[test]
    fn qualifies_table_names() {
        let table = QualifiedTable::new("public", "load_test").unwrap();
        assert_eq!(table.display(), "public.load_test");
        assert_eq!(table.quoted(), "public.load_test");
    }

    #[test]
    fn payload_uses_csv_safe_ascii() {
        let payload = make_payload(1024, 42);
        assert_eq!(payload.len(), 1024);
        assert!(!payload.contains(','));
        assert!(!payload.contains('"'));
        assert!(!payload.contains('\n'));
    }
}
