use std::{
    env,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::utils::workspace_root;

/// Migration directory for etl-api public schema migrations.
const ETL_API_MIGRATIONS_DIR: &str = "crates/etl-api/migrations";
/// Migration directory for source database migrations.
const ETL_SOURCE_MIGRATIONS_DIR: &str = "crates/etl/migrations/source";
/// Migration directory for Postgres store migrations.
const ETL_STORE_MIGRATIONS_DIR: &str = "crates/etl/migrations/postgres_store";
/// SQLx CLI installation command shown when SQLx is missing.
const SQLX_INSTALL_COMMAND: &str = "cargo install --version 0.9.0-alpha.1 sqlx-cli \
                                    --no-default-features --features rustls,postgres --locked";
/// SQLx connection option that makes the migration table live in the etl
/// schema.
const SQLX_MIGRATIONS_OPTIONS: &str = "options=-csearch_path%3Detl";
/// Help text for the migrate command.
const MIGRATE_USAGE: &str = "\
Usage: cargo x migrate [OPTIONS] [TARGETS...]

Run database migrations for etl components.

Targets:
  etl-api    Run etl-api migrations (public schema)
  etl        Run etl source and Postgres store migrations (etl schema)
  all        Run all migrations (default if no target specified)

Options:
  -h, --help    Show this help message

Examples:
  cargo x migrate              # Run all migrations
  cargo x migrate etl-api      # Run only etl-api migrations
  cargo x migrate etl          # Run only etl migrations
  cargo x migrate etl-api etl  # Run both explicitly
";

/// Arguments for running database migrations.
#[derive(Args)]
#[command(disable_help_flag = true)]
pub(crate) struct MigrateArgs {
    /// Migration targets (etl-api, etl, all).
    #[arg(value_name = "TARGETS", trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl MigrateArgs {
    /// Runs the selected database migrations.
    pub(crate) fn run(self) -> Result<()> {
        run_migrations(&self.args)
    }
}

/// Runs database migrations with the provided migration targets.
pub(super) fn run_migrations(args: &[String]) -> Result<()> {
    run_migrations_with_env(args, std::iter::empty::<(&str, &str)>())
}

/// Runs database migrations with extra environment variables.
pub(super) fn run_migrations_with_env<'a>(
    args: &[String],
    envs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<()> {
    let selection = match MigrationSelection::parse(args)? {
        MigrationAction::Help => {
            print_usage();
            return Ok(());
        }
        MigrationAction::Run(selection) => selection,
    };

    let workspace_root = workspace_root()?;
    let env_overrides = EnvOverrides::new(envs);
    let database = DatabaseEnv::from_env(&workspace_root, &env_overrides);

    require_command("sqlx", &["--version"], "SQLx CLI is not installed")?;
    if selection.run_etl {
        require_command("psql", &["--version"], "Postgres client (psql) is not installed")?;
    }

    if selection.run_etl_api {
        run_etl_api_migrations(&workspace_root, &database)?;
    }

    if selection.run_etl {
        run_etl_migrations(&workspace_root, &database)?;
    }

    println!("All requested migrations complete!");
    Ok(())
}

/// Selected migration targets.
struct MigrationSelection {
    /// Whether to run etl-api migrations.
    run_etl_api: bool,
    /// Whether to run etl migrations.
    run_etl: bool,
}

impl MigrationSelection {
    /// Parses target arguments into a migration selection.
    fn parse(args: &[String]) -> Result<MigrationAction> {
        if args.is_empty() {
            return Ok(MigrationAction::Run(Self { run_etl_api: true, run_etl: true }));
        }

        let mut selection = Self { run_etl_api: false, run_etl: false };
        for arg in args {
            match arg.as_str() {
                "-h" | "--help" => return Ok(MigrationAction::Help),
                "etl-api" => selection.run_etl_api = true,
                "etl" => selection.run_etl = true,
                "all" => {
                    selection.run_etl_api = true;
                    selection.run_etl = true;
                }
                _ => {
                    eprintln!("Error: Unknown argument '{arg}'");
                    print_usage_to_stderr();
                    bail!("Unknown migration argument `{arg}`");
                }
            }
        }

        Ok(MigrationAction::Run(selection))
    }
}

/// Parsed migration command action.
enum MigrationAction {
    /// Print command usage.
    Help,
    /// Run the selected migrations.
    Run(MigrationSelection),
}

/// Environment variable overrides for nested xtask calls.
struct EnvOverrides {
    /// Overridden environment variable values.
    values: Vec<(String, String)>,
}

impl EnvOverrides {
    /// Creates an override set from key-value pairs.
    fn new<'a>(envs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let values =
            envs.into_iter().map(|(key, value)| (key.to_owned(), value.to_owned())).collect();
        Self { values }
    }

    /// Returns an override value.
    fn get(&self, key: &str) -> Option<&str> {
        self.values
            .iter()
            .rev()
            .find_map(|(env_key, value)| (env_key == key).then_some(value.as_str()))
    }
}

/// Database settings used by migration commands.
struct DatabaseEnv {
    /// Postgres user.
    db_user: String,
    /// Postgres password.
    db_password: String,
    /// Postgres database name.
    db_name: String,
    /// Postgres host port.
    db_port: String,
    /// Postgres host.
    db_host: String,
    /// SQLx and psql database URL.
    database_url: String,
    /// Optional libpq SSL mode.
    pg_ssl_mode: Option<String>,
    /// Optional libpq SSL root certificate path.
    pg_ssl_root_cert: Option<String>,
}

impl DatabaseEnv {
    /// Loads database settings from environment variables.
    fn from_env(workspace_root: &Path, env_overrides: &EnvOverrides) -> Self {
        let db_user = env_or("POSTGRES_USER", "postgres", env_overrides);
        let db_password = env_or("POSTGRES_PASSWORD", "postgres", env_overrides);
        let db_name = env_or("POSTGRES_DB", "postgres", env_overrides);
        let db_port = env_or("POSTGRES_PORT", "5430", env_overrides);
        let db_host = env_or("POSTGRES_HOST", "localhost", env_overrides);

        let mut database_url =
            format!("postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}");
        let mut pg_ssl_mode = None;
        let mut pg_ssl_root_cert = None;

        if tls_enabled(env_overrides) {
            let default_root_cert =
                workspace_root.join("target/postgres-tls/root.crt").display().to_string();
            let ssl_root_cert =
                env_or("TESTS_DATABASE_TLS_ROOT_CERT", &default_root_cert, env_overrides);
            database_url.push_str("?sslmode=verify-full&sslrootcert=");
            database_url.push_str(&ssl_root_cert);
            pg_ssl_mode = Some("verify-full".to_owned());
            pg_ssl_root_cert = Some(ssl_root_cert);
        }

        Self {
            db_user,
            db_password,
            db_name,
            db_port,
            db_host,
            database_url,
            pg_ssl_mode,
            pg_ssl_root_cert,
        }
    }

    /// Adds database environment variables to a command.
    fn apply_to(&self, cmd: &mut Command) {
        cmd.env("POSTGRES_USER", &self.db_user);
        cmd.env("POSTGRES_PASSWORD", &self.db_password);
        cmd.env("POSTGRES_DB", &self.db_name);
        cmd.env("POSTGRES_PORT", &self.db_port);
        cmd.env("POSTGRES_HOST", &self.db_host);
        cmd.env("DATABASE_URL", &self.database_url);

        if let Some(pg_ssl_mode) = &self.pg_ssl_mode {
            cmd.env("PGSSLMODE", pg_ssl_mode);
        }
        if let Some(pg_ssl_root_cert) = &self.pg_ssl_root_cert {
            cmd.env("PGSSLROOTCERT", pg_ssl_root_cert);
        }
    }
}

/// Runs etl-api migrations.
fn run_etl_api_migrations(workspace_root: &Path, database: &DatabaseEnv) -> Result<()> {
    let migrations_dir = workspace_root.join(ETL_API_MIGRATIONS_DIR);
    ensure_migrations_dir(&migrations_dir, ETL_API_MIGRATIONS_DIR)?;

    println!("Running etl-api migrations...");

    let mut create = Command::new("sqlx");
    create.args(["database", "create"]);
    database.apply_to(&mut create);
    run_command(create, "Failed to create etl-api database")?;

    let mut migrate = Command::new("sqlx");
    migrate.args(["migrate", "run", "--source"]);
    migrate.arg(&migrations_dir);
    database.apply_to(&mut migrate);
    run_command(migrate, "Failed to run etl-api migrations")?;

    println!("etl-api migrations complete!");
    Ok(())
}

/// Runs etl source and Postgres store migrations.
fn run_etl_migrations(workspace_root: &Path, database: &DatabaseEnv) -> Result<()> {
    let source_migrations_dir = workspace_root.join(ETL_SOURCE_MIGRATIONS_DIR);
    let store_migrations_dir = workspace_root.join(ETL_STORE_MIGRATIONS_DIR);
    ensure_migrations_dir(&source_migrations_dir, ETL_SOURCE_MIGRATIONS_DIR)?;
    ensure_migrations_dir(&store_migrations_dir, ETL_STORE_MIGRATIONS_DIR)?;

    println!("Running etl migrations...");

    let mut create_schema = Command::new("psql");
    create_schema
        .arg(&database.database_url)
        .args(["-v", "ON_ERROR_STOP=1", "-c", "create schema if not exists etl;"])
        .stdout(Stdio::null());
    database.apply_to(&mut create_schema);
    run_command(create_schema, "Failed to create etl schema")?;

    let migration_url = migration_url_for_etl_schema(&database.database_url);

    let mut create_database = Command::new("sqlx");
    create_database.args(["database", "create", "--database-url", &database.database_url]);
    database.apply_to(&mut create_database);
    run_command(create_database, "Failed to create etl database")?;

    let mut store_migrations = Command::new("sqlx");
    store_migrations.args(["migrate", "run", "--source"]);
    store_migrations.arg(&store_migrations_dir);
    store_migrations.args(["--database-url", &migration_url, "--ignore-missing"]);
    database.apply_to(&mut store_migrations);
    run_command(store_migrations, "Failed to run etl Postgres store migrations")?;

    let mut source_migrations = Command::new("sqlx");
    source_migrations.args(["migrate", "run", "--source"]);
    source_migrations.arg(&source_migrations_dir);
    source_migrations.args(["--database-url", &migration_url, "--ignore-missing"]);
    database.apply_to(&mut source_migrations);
    run_command(source_migrations, "Failed to run etl source migrations")?;

    println!("etl migrations complete!");
    Ok(())
}

/// Verifies that a migrations directory exists.
fn ensure_migrations_dir(path: &Path, display_path: &str) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }

    bail!("'{display_path}' folder not found at {}", path.display());
}

/// Returns a database URL that sets SQLx migrations search path to etl.
fn migration_url_for_etl_schema(database_url: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}{SQLX_MIGRATIONS_OPTIONS}")
}

/// Requires a command to be callable.
fn require_command(program: &str, args: &[&str], message: &'static str) -> Result<()> {
    if command_succeeds(program, args) {
        return Ok(());
    }

    if program == "sqlx" {
        bail!("{message}.\nTo install it, run:\n    {SQLX_INSTALL_COMMAND}");
    }

    bail!("{message}");
}

/// Returns whether a command exits successfully.
fn command_succeeds(program: &str, args: &[&str]) -> bool {
    let mut cmd = Command::new(program);
    cmd.args(args);
    command_succeeds_silent(&mut cmd).unwrap_or(false)
}

/// Returns whether a command exits successfully while discarding output.
fn command_succeeds_silent(cmd: &mut Command) -> Result<bool> {
    let status = cmd.stdout(Stdio::null()).stderr(Stdio::null()).status()?;
    Ok(status.success())
}

/// Runs a command and fails when it exits unsuccessfully.
fn run_command(mut cmd: Command, context: &'static str) -> Result<()> {
    let status = cmd.status().context(context)?;

    if !status.success() {
        bail!("{context}");
    }

    Ok(())
}

/// Returns an environment variable or a default value.
fn env_or(key: &str, default: &str, env_overrides: &EnvOverrides) -> String {
    env_overrides
        .get(key)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| env::var(key).ok().filter(|value| !value.is_empty()))
        .unwrap_or_else(|| default.to_owned())
}

/// Returns whether TLS is enabled for the test database connection.
fn tls_enabled(env_overrides: &EnvOverrides) -> bool {
    matches!(
        env_or("TESTS_DATABASE_TLS_ENABLED", "false", env_overrides).as_str(),
        "1" | "true" | "TRUE" | "yes" | "on"
    )
}

/// Prints command usage to stdout.
fn print_usage() {
    print!("{MIGRATE_USAGE}");
}

/// Prints command usage to stderr.
fn print_usage_to_stderr() {
    eprint!("{MIGRATE_USAGE}");
}
