mod commands;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{
    BenchmarkArgs, BenchmarkCompareArgs, ChaosArgs, CheckArgs, DeployLocalArgs, ExampleArgs,
    FixArgs, FmtArgs, InitArgs, MigrateArgs, MsrvArgs, NextestArgs, PostgresArgs,
    RotateEncryptionKeyArgs, SeedArgs, TestArgs, TestClickhouseArgs, TestSnowflakeArgs,
    VendorDuckdbArgs,
};

#[derive(Parser)]
#[command(name = "xtask", about = "Project task runner")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Prepare and run ETL benchmarks.
    Benchmark(Box<BenchmarkArgs>),
    /// Compare benchmark reports with a previous run.
    #[command(name = "benchmark-compare")]
    BenchmarkCompare(BenchmarkCompareArgs),
    /// Run chaos testing scenarios against the Kubernetes cluster.
    Chaos(ChaosArgs),
    /// Pre-PR gate: fmt, sort, clippy.
    Check(CheckArgs),
    /// Deploy the replicator to a local OrbStack Kubernetes cluster.
    #[command(name = "deploy-local")]
    DeployLocal(DeployLocalArgs),
    /// Run a destination example (e.g. `cargo x example snowflake`).
    Example(ExampleArgs),
    /// Auto-fix: clippy --fix, fmt, sort.
    Fix(FixArgs),
    /// Format code with nightly rustfmt.
    Fmt(FmtArgs),
    /// Set up the local development environment.
    Init(InitArgs),
    /// Run database migrations.
    Migrate(MigrateArgs),
    /// Verify MSRV consistency across Cargo.toml, rust-toolchain.toml, and
    /// cargo-msrv.
    Msrv(MsrvArgs),
    /// Run tests via nextest, sharded across multiple Postgres clusters.
    Nextest(NextestArgs),
    /// Manage test Postgres clusters.
    Postgres(PostgresArgs),
    /// Re-encrypt API source and destination configs with the latest configured
    /// key.
    #[command(name = "rotate-encryption-key")]
    RotateEncryptionKey(RotateEncryptionKeyArgs),
    /// Seed a Postgres database with test tables and data for destination
    /// examples.
    Seed(SeedArgs),
    /// Run local tests via nextest.
    Test(TestArgs),
    /// Run ClickHouse integration tests with a local Docker setup.
    #[command(name = "test-clickhouse")]
    TestClickhouse(TestClickhouseArgs),
    /// Run Snowflake tests, including integration tests (requiring credentials)
    /// when configured.
    #[command(name = "test-snowflake")]
    TestSnowflake(TestSnowflakeArgs),
    /// Download and vendor DuckDB extensions.
    #[command(name = "vendor-duckdb")]
    VendorDuckdb(VendorDuckdbArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Benchmark(cmd) => cmd.run(),
        Command::BenchmarkCompare(cmd) => cmd.run().await,
        Command::Chaos(cmd) => cmd.run().await,
        Command::Check(cmd) => cmd.run(),
        Command::DeployLocal(cmd) => cmd.run(),
        Command::Example(cmd) => cmd.run(),
        Command::Fix(cmd) => cmd.run(),
        Command::Fmt(cmd) => cmd.run(),
        Command::Init(cmd) => cmd.run(),
        Command::Migrate(cmd) => cmd.run(),
        Command::Msrv(cmd) => cmd.run(),
        Command::Nextest(cmd) => cmd.run(),
        Command::Postgres(cmd) => cmd.run(),
        Command::RotateEncryptionKey(cmd) => cmd.run().await,
        Command::Seed(cmd) => cmd.run(),
        Command::Test(cmd) => cmd.run(),
        Command::TestClickhouse(cmd) => cmd.run(),
        Command::TestSnowflake(cmd) => cmd.run(),
        Command::VendorDuckdb(cmd) => cmd.run(),
    }
}
