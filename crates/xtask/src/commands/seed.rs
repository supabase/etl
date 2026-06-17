use anyhow::{Context, Result, bail};
use clap::Args;
use pg_escape::{quote_identifier, quote_literal};
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct SeedArgs {
    /// PostgreSQL host.
    #[arg(long, env = "TESTS_DATABASE_HOST", default_value = "localhost")]
    host: String,

    /// PostgreSQL port.
    #[arg(long, env = "TESTS_DATABASE_PORT", default_value = "5430")]
    port: u16,

    /// PostgreSQL user.
    #[arg(long, env = "TESTS_DATABASE_USERNAME", default_value = "postgres")]
    user: String,

    /// PostgreSQL password.
    #[arg(long, env = "TESTS_DATABASE_PASSWORD", default_value = "postgres")]
    password: String,

    /// Database name to create and seed.
    #[arg(long, default_value = "etl_testdata")]
    database: String,

    /// Number of rows per table (users get rows/10, orders and events get
    /// rows).
    #[arg(long, default_value = "1000")]
    rows: u64,

    /// Publication name to create.
    #[arg(long, default_value = "seed_pub")]
    publication: String,

    /// Drop and recreate the database if it already exists.
    #[arg(long)]
    force: bool,
}

impl SeedArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        if cmd!(sh, "which psql").quiet().run().is_err() {
            bail!("psql is not installed or not in PATH");
        }

        sh.set_var("PGPASSWORD", &self.password);

        let host = &self.host;
        let port = self.port.to_string();
        let user = &self.user;
        let database = &self.database;
        let publication = &self.publication;
        let user_count = (self.rows / 10).max(10);

        let check_sql = database_exists_sql(database)?;
        let exists =
            cmd!(sh, "psql -q -h {host} -p {port} -U {user} -d postgres -tA -c {check_sql}")
                .quiet()
                .read()
                .unwrap_or_default();
        let db_exists = exists.trim() == "1";

        if db_exists && !self.force {
            bail!("database {database} already exists (use --force to drop and recreate)");
        }

        if db_exists {
            println!("[seed] dropping database {database} (--force)...");
            let drop_sql = drop_database_sql(database)?;
            cmd!(sh, "psql -q -h {host} -p {port} -U {user} -d postgres -c {drop_sql}")
                .quiet()
                .run()
                .context("failed to drop database")?;
        }

        println!("[seed] creating database {database}...");
        let create_db = create_database_sql(database)?;
        cmd!(sh, "psql -q -h {host} -p {port} -U {user} -d postgres -c {create_db}")
            .quiet()
            .run()
            .context("failed to create database")?;

        let sql = seed_sql(self.rows, user_count, publication)?;

        println!(
            "[seed] creating tables and seeding {rows} rows ({user_count} users, {rows} orders, \
             {rows} events)...",
            rows = self.rows
        );

        cmd!(sh, "psql -q -h {host} -p {port} -U {user} -d {database} -c {sql}")
            .quiet()
            .run()
            .context("failed to seed database")?;

        let count_sql = "SELECT 'users: ' || count(*) FROM users UNION ALL SELECT 'orders: ' || \
                         count(*) FROM orders UNION ALL SELECT 'events: ' || count(*) FROM events";
        let counts =
            cmd!(sh, "psql -q -h {host} -p {port} -U {user} -d {database} -t -c {count_sql}")
                .quiet()
                .read()
                .unwrap_or_default();

        println!("[seed] done!");
        for line in counts.lines() {
            let line = line.trim();
            if !line.is_empty() {
                println!("  {line}");
            }
        }
        println!("[seed] publication: {publication}");
        println!("[seed] connect with: psql -h {host} -p {port} -U {user} -d {database}");

        Ok(())
    }
}

fn database_exists_sql(database: &str) -> Result<String> {
    ensure_database_name(database)?;
    Ok(format!("SELECT 1 FROM pg_database WHERE datname = {}", quote_literal(database)))
}

fn drop_database_sql(database: &str) -> Result<String> {
    ensure_database_name(database)?;
    Ok(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", quote_identifier(database)))
}

fn create_database_sql(database: &str) -> Result<String> {
    ensure_database_name(database)?;
    Ok(format!("CREATE DATABASE {}", quote_identifier(database)))
}

fn seed_sql(rows: u64, user_count: u64, publication: &str) -> Result<String> {
    ensure_publication_name(publication)?;
    let publication = quote_identifier(publication);

    Ok(format!(
        r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    total NUMERIC(10,2),
    status TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    event_type TEXT NOT NULL,
    amount NUMERIC(12,2),
    metadata JSONB,
    tags TEXT[],
    is_active BOOLEAN DEFAULT true,
    score DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    ip_address TEXT,
    notes TEXT
);

INSERT INTO users (name, email, created_at)
SELECT
    'User ' || n,
    'user' || n || '@example.com',
    now() - (random() * interval '365 days')
FROM generate_series(1, {user_count}) n;

INSERT INTO orders (user_id, total, status, created_at)
SELECT
    (random() * ({user_count} - 1) + 1)::int,
    (random() * 999.99 + 0.01)::numeric(10,2),
    (ARRAY['pending', 'completed', 'cancelled', 'refunded'])[1 + (random() * 3)::int],
    now() - (random() * interval '365 days')
FROM generate_series(1, {rows}) n;

INSERT INTO events (user_id, event_type, amount, metadata, tags, is_active, score, created_at, updated_at, ip_address, notes)
SELECT
    (random() * ({user_count} - 1) + 1)::int,
    (ARRAY['page_view', 'click', 'purchase', 'signup', 'logout', 'search', 'download', 'share'])[1 + (random() * 7)::int],
    (random() * 999.99)::numeric(12,2),
    json_build_object('source', 'web', 'version', (random() * 9 + 1)::int)::jsonb,
    ARRAY[(ARRAY['rust', 'web', 'mobile', 'api', 'beta', 'premium', 'trial'])[1 + (random() * 6)::int]],
    random() > 0.1,
    (random() * 100)::numeric(5,2)::double precision,
    now() - (random() * interval '365 days'),
    now() - (random() * interval '30 days'),
    '192.168.' || (random() * 255)::int || '.' || (random() * 254 + 1)::int,
    ''
FROM generate_series(1, {rows}) n;

CREATE PUBLICATION {publication} FOR TABLE users, orders, events;
"#
    ))
}

fn ensure_database_name(database: &str) -> Result<()> {
    ensure_identifier_is_not_empty("Database", database)
}

fn ensure_publication_name(publication: &str) -> Result<()> {
    ensure_identifier_is_not_empty("Publication", publication)
}

fn ensure_identifier_is_not_empty(kind: &str, identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        bail!("{kind} name cannot be empty");
    }

    Ok(())
}
