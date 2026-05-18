use std::{error::Error, time::Instant};

use clap::{Parser, Subcommand};
use rand::Rng;
use tokio::signal;
use tokio_postgres::NoTls;

#[derive(Clone, Copy)]
enum Op {
    Insert,
    Update,
    Delete,
}

const EVENT_TYPES: &[&str] =
    &["page_view", "click", "purchase", "signup", "logout", "search", "download", "share"];

const IP_PREFIXES: &[&str] = &["192.168.", "10.0.", "172.16.", "203.0.113."];

#[derive(Debug, Parser)]
#[command(
    name = "snowflake-loadgen",
    version,
    about = "Load generator for Snowflake ETL benchmarking"
)]
struct AppArgs {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Seed(SeedArgs),
    Generate(GenerateArgs),
}

#[derive(Debug, Parser)]
struct SeedArgs {
    #[arg(long)]
    db_url: String,
    #[arg(long, default_value = "1000000")]
    rows: u64,
}

#[derive(Debug, Parser)]
struct GenerateArgs {
    #[arg(long)]
    db_url: String,
    #[arg(long, default_value = "100")]
    rate: u64,
    /// Insert/update/delete percentages, e.g. "40/40/20"
    #[arg(long, default_value = "40/40/20")]
    mix: String,
    /// Duration to run, e.g. "300s" or "5m". Omit to run indefinitely.
    #[arg(long)]
    duration: Option<String>,
}

fn random_event_type(rng: &mut impl Rng) -> &'static str {
    EVENT_TYPES[rng.random_range(0..EVENT_TYPES.len())]
}

fn random_amount(rng: &mut impl Rng) -> f64 {
    (rng.random_range(0u64..100_000) as f64) / 100.0
}

fn random_jsonb(rng: &mut impl Rng) -> String {
    format!(r#"{{"source":"web","version":{}}}"#, rng.random_range(1..10))
}

fn random_ip(rng: &mut impl Rng) -> String {
    let prefix = IP_PREFIXES[rng.random_range(0..IP_PREFIXES.len())];
    format!("{}{}.{}", prefix, rng.random_range(0..256), rng.random_range(1..255))
}

fn random_tags(rng: &mut impl Rng) -> String {
    let all = &["rust", "web", "mobile", "api", "beta", "premium", "trial"];
    let count = rng.random_range(0..4usize);
    let tags: Vec<&str> = (0..count).map(|_| all[rng.random_range(0..all.len())]).collect();
    format!("{{{}}}", tags.join(","))
}

fn parse_duration(s: &str) -> Result<std::time::Duration, Box<dyn Error>> {
    if let Some(stripped) = s.strip_suffix('s') {
        let secs: u64 = stripped.parse()?;
        return Ok(std::time::Duration::from_secs(secs));
    }
    if let Some(stripped) = s.strip_suffix('m') {
        let mins: u64 = stripped.parse()?;
        return Ok(std::time::Duration::from_secs(mins * 60));
    }
    if let Some(stripped) = s.strip_suffix('h') {
        let hours: u64 = stripped.parse()?;
        return Ok(std::time::Duration::from_secs(hours * 3600));
    }
    Err(format!("invalid duration '{}': expected format like '300s', '5m', or '2h'", s).into())
}

fn parse_mix(s: &str) -> Result<(u32, u32, u32), Box<dyn Error>> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 3 {
        return Err(format!("invalid mix '{}': expected format like '40/40/20'", s).into());
    }
    let insert: u32 = parts[0].parse()?;
    let update: u32 = parts[1].parse()?;
    let delete: u32 = parts[2].parse()?;
    if insert + update + delete != 100 {
        return Err(
            format!("mix percentages must sum to 100, got {}", insert + update + delete).into()
        );
    }
    Ok((insert, update, delete))
}

async fn seed(client: &tokio_postgres::Client, rows: u64) -> Result<(), Box<dyn Error>> {
    let mut rng = rand::rng();

    // Seed users (10% of rows, min 100)
    let user_count = (rows / 10).max(100);
    let user_batch = 5000u64;
    let mut users_inserted = 0u64;

    eprintln!("Seeding {} users...", user_count);
    while users_inserted < user_count {
        let batch = user_batch.min(user_count - users_inserted);
        let mut values = Vec::with_capacity(batch as usize);
        for i in 0..batch {
            let n = users_inserted + i;
            let name = format!("User {n}");
            let email = format!("user{}@example.com", n);
            values.push(format!("('{name}', '{email}', now())"));
        }
        let sql = format!(
            "INSERT INTO bench.users (name, email, created_at) VALUES {}",
            values.join(",")
        );
        client.execute(&sql, &[]).await?;
        users_inserted += batch;
    }
    eprintln!("  users done: {}", users_inserted);

    // Get user ID range
    let row = client.query_one("SELECT MIN(id), MAX(id) FROM bench.users", &[]).await?;
    let min_uid: i32 = row.get(0);
    let max_uid: i32 = row.get(1);

    // Seed orders (same count as rows)
    let order_batch = 5000u64;
    let mut orders_inserted = 0u64;
    let statuses = &["pending", "completed", "cancelled", "refunded"];

    eprintln!("Seeding {} orders...", rows);
    while orders_inserted < rows {
        let batch = order_batch.min(rows - orders_inserted);
        let mut values = Vec::with_capacity(batch as usize);
        for _ in 0..batch {
            let uid = rng.random_range(min_uid..=max_uid);
            let total = random_amount(&mut rng);
            let status = statuses[rng.random_range(0..statuses.len())];
            values.push(format!("({uid}, {total:.2}, '{status}', now())"));
        }
        let sql = format!(
            "INSERT INTO bench.orders (user_id, total, status, created_at) VALUES {}",
            values.join(",")
        );
        client.execute(&sql, &[]).await?;
        orders_inserted += batch;
        if orders_inserted % 10_000 == 0 || orders_inserted == rows {
            eprintln!("  orders {}/{}", orders_inserted, rows);
        }
    }

    // Seed events
    let event_batch = 5000u64;
    let mut events_inserted = 0u64;

    eprintln!("Seeding {} events...", rows);
    while events_inserted < rows {
        let batch = event_batch.min(rows - events_inserted);
        let mut values = Vec::with_capacity(batch as usize);
        for _ in 0..batch {
            let uid = rng.random_range(min_uid..=max_uid);
            let event_type = random_event_type(&mut rng);
            let amount = random_amount(&mut rng);
            let metadata = random_jsonb(&mut rng);
            let tags = random_tags(&mut rng);
            let score: f64 = rng.random_range(0..10000) as f64 / 100.0;
            let ip = random_ip(&mut rng);
            values.push(format!(
                "({uid}, '{event_type}', {amount:.2}, '{metadata}', '{tags}', true, {score:.2}, \
                 now(), now(), '{ip}', '')"
            ));
        }
        let sql = format!(
            "INSERT INTO bench.events (user_id, event_type, amount, metadata, tags, is_active, \
             score, created_at, updated_at, ip_address, notes) VALUES {}",
            values.join(",")
        );
        client.execute(&sql, &[]).await?;
        events_inserted += batch;
        if events_inserted % 10_000 == 0 || events_inserted == rows {
            eprintln!("  events {}/{}", events_inserted, rows);
        }
    }

    Ok(())
}

async fn run_seed(args: SeedArgs) -> Result<(), Box<dyn Error>> {
    eprintln!("Connecting to Postgres...");
    let (client, connection) = tokio_postgres::connect(&args.db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    eprintln!("Creating bench schema (users, orders, events)...");
    client
        .batch_execute(
            "DROP SCHEMA IF EXISTS bench CASCADE;
             DROP PUBLICATION IF EXISTS bench_pub;
             CREATE SCHEMA bench;
             CREATE TABLE bench.users (
                 id SERIAL PRIMARY KEY,
                 name TEXT NOT NULL,
                 email TEXT NOT NULL,
                 created_at TIMESTAMPTZ DEFAULT now()
             );
             CREATE TABLE bench.orders (
                 id SERIAL PRIMARY KEY,
                 user_id INT REFERENCES bench.users(id),
                 total NUMERIC(10,2),
                 status TEXT,
                 created_at TIMESTAMPTZ DEFAULT now()
             );
             CREATE TABLE bench.events (
                 id BIGSERIAL PRIMARY KEY,
                 user_id INT REFERENCES bench.users(id),
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
             )",
        )
        .await?;

    eprintln!("Seeding {} rows...", args.rows);
    let start = Instant::now();
    seed(&client, args.rows).await?;

    let elapsed = start.elapsed();

    eprintln!("Creating publication bench_pub...");
    client
        .execute("CREATE PUBLICATION bench_pub FOR TABLES IN SCHEMA bench", &[])
        .await
        .or_else(|e| if e.to_string().contains("already exists") { Ok(0) } else { Err(e) })?;

    eprintln!(
        "Done. Inserted {} rows in {:.1}s ({:.0} rows/sec).",
        args.rows,
        elapsed.as_secs_f64(),
        args.rows as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn run_generate(args: GenerateArgs) -> Result<(), Box<dyn Error>> {
    let (insert_pct, update_pct, _delete_pct) = parse_mix(&args.mix)?;
    let deadline = args.duration.as_deref().map(parse_duration).transpose()?;

    eprintln!("Connecting to Postgres...");
    let (client, connection) = tokio_postgres::connect(&args.db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Cache ID ranges for random lookups (avoid ORDER BY random() scans)
    let id_range = client.query_one("SELECT MIN(id), MAX(id) FROM bench.events", &[]).await?;
    let mut min_id: i64 = id_range.get(0);
    let mut max_id: i64 = id_range.get(1);

    let user_row = client.query_one("SELECT MIN(id), MAX(id) FROM bench.users", &[]).await?;
    let min_uid: i32 = user_row.get(0);
    let max_uid: i32 = user_row.get(1);

    // Batch size: target rate / batches_per_sec. At 5000 ops/sec with 50
    // batches/sec = 100 ops/batch.
    let batch_size = (args.rate / 50).clamp(1, 500) as usize;
    let batches_per_sec = (args.rate as f64 / batch_size as f64).ceil() as u64;
    let interval_us = 1_000_000u64 / batches_per_sec.max(1);
    let mut ticker = tokio::time::interval(std::time::Duration::from_micros(interval_us));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let start = Instant::now();
    let mut total_ops: u64 = 0;
    let mut last_report = Instant::now();
    let mut ops_since_report: u64 = 0;
    let mut rng = rand::rng();

    let ctrl_c = signal::ctrl_c();
    tokio::pin!(ctrl_c);

    eprintln!(
        "Generating CDC changes at {} ops/sec (batch={}, mix: {}){}",
        args.rate,
        batch_size,
        args.mix,
        match deadline {
            Some(d) => format!(", duration: {}s", d.as_secs()),
            None => ", running until ctrl-c".to_string(),
        }
    );

    loop {
        tokio::select! {
            biased;
            _ = &mut ctrl_c => {
                eprintln!("\nReceived ctrl-c, stopping.");
                break;
            }
            _ = ticker.tick() => {}
        }

        if let Some(d) = deadline {
            if start.elapsed() >= d {
                eprintln!("Duration reached, stopping.");
                break;
            }
        }

        // Build a batch of operations as a single SQL string
        let mut stmts: Vec<String> = Vec::with_capacity(batch_size);
        let mut insert_values: Vec<String> = Vec::new();
        let mut batch_ops = 0u64;

        for _ in 0..batch_size {
            let roll: u32 = rng.random_range(0..100);
            let op = if roll < insert_pct {
                Op::Insert
            } else if roll < insert_pct + update_pct {
                Op::Update
            } else {
                Op::Delete
            };

            match op {
                Op::Insert => {
                    let uid = rng.random_range(min_uid..=max_uid);
                    let event_type = random_event_type(&mut rng);
                    let amount = random_amount(&mut rng);
                    let metadata = random_jsonb(&mut rng);
                    let tags = random_tags(&mut rng);
                    let score: f64 = rng.random_range(0..10000) as f64 / 100.0;
                    let ip = random_ip(&mut rng);
                    insert_values.push(format!(
                        "({uid}, '{event_type}', {amount:.2}, '{metadata}', '{tags}', true, \
                         {score:.2}, now(), now(), '{ip}', '')"
                    ));
                }
                Op::Update => {
                    let target_id = rng.random_range(min_id..=max_id);
                    let score: f64 = rng.random_range(0..10000) as f64 / 100.0;
                    let event_type = random_event_type(&mut rng);
                    stmts.push(format!(
                        "UPDATE bench.events SET score = {score:.2}, event_type = '{event_type}', \
                         updated_at = now() WHERE id = {target_id}"
                    ));
                }
                Op::Delete => {
                    let target_id = rng.random_range(min_id..=max_id);
                    stmts.push(format!("DELETE FROM bench.events WHERE id = {target_id}"));
                }
            }
            batch_ops += 1;
        }

        // Flush accumulated inserts as one multi-row INSERT
        if !insert_values.is_empty() {
            let cols = "user_id, event_type, amount, metadata, tags, is_active, score, \
                        created_at, updated_at, ip_address, notes";
            stmts.insert(
                0,
                format!("INSERT INTO bench.events ({cols}) VALUES {}", insert_values.join(",")),
            );
        }

        if stmts.is_empty() {
            continue;
        }

        let sql = stmts.join("; ");
        match client.batch_execute(&sql).await {
            Ok(()) => {
                total_ops += batch_ops;
                ops_since_report += batch_ops;
                // Track max_id growth from inserts
                max_id += insert_values.len() as i64;
            }
            Err(e) => {
                eprintln!("batch error: {e}");
            }
        }

        let since_report = last_report.elapsed();
        if since_report >= std::time::Duration::from_secs(5) {
            let throughput = ops_since_report as f64 / since_report.as_secs_f64();
            eprintln!(
                "[{:.0}s] {:.0} ops/sec (total: {})",
                start.elapsed().as_secs_f64(),
                throughput,
                total_ops
            );
            ops_since_report = 0;
            last_report = Instant::now();

            // Refresh ID range periodically to account for inserts/deletes
            if let Ok(row) =
                client.query_one("SELECT MIN(id), MAX(id) FROM bench.events", &[]).await
            {
                min_id = row.get(0);
                max_id = row.get(1);
            }
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "Finished. {} ops in {:.1}s ({:.0} ops/sec average).",
        total_ops,
        elapsed.as_secs_f64(),
        total_ops as f64 / elapsed.as_secs_f64().max(0.001)
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = AppArgs::parse();
    match args.command {
        Command::Seed(seed_args) => run_seed(seed_args).await?,
        Command::Generate(gen_args) => run_generate(gen_args).await?,
    }

    Ok(())
}
