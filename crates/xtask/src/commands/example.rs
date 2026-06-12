use anyhow::{Result, bail};
use clap::Args;
use xshell::{Shell, cmd};

const KNOWN_EXAMPLES: &[&str] = &["bigquery", "clickhouse", "ducklake", "snowflake"];

#[derive(Args)]
pub(crate) struct ExampleArgs {
    /// Example name (bigquery, clickhouse, ducklake, snowflake).
    name: String,

    /// Extra arguments passed through to the example binary.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl ExampleArgs {
    pub(crate) fn run(self) -> Result<()> {
        let name = &self.name;

        if !KNOWN_EXAMPLES.contains(&name.as_str()) {
            bail!("unknown example '{name}'. Available: {}", KNOWN_EXAMPLES.join(", "));
        }

        let db_host = env_or("TESTS_DATABASE_HOST", None);
        let db_port = env_or("TESTS_DATABASE_PORT", None);
        let db_username = env_or("TESTS_DATABASE_USERNAME", None);
        let db_password = env_or("TESTS_DATABASE_PASSWORD", None);

        let missing: Vec<&str> = [
            db_host.is_none().then_some("TESTS_DATABASE_HOST"),
            db_port.is_none().then_some("TESTS_DATABASE_PORT"),
            db_username.is_none().then_some("TESTS_DATABASE_USERNAME"),
        ]
        .into_iter()
        .flatten()
        .collect();

        if !missing.is_empty() && !has_flag(&self.args, "--db-host") {
            bail!(
                "missing environment variables: {}\n\nSet them in .env and run: source .env\nSee \
                 .env.example for details.",
                missing.join(", ")
            );
        }

        let mut extra: Vec<String> = Vec::new();

        inject(&mut extra, "--db-host", &db_host, &self.args);
        inject(&mut extra, "--db-port", &db_port, &self.args);
        inject(&mut extra, "--db-username", &db_username, &self.args);
        inject(&mut extra, "--db-password", &db_password, &self.args);
        inject(&mut extra, "--db-name", &Some("etl_testdata".to_owned()), &self.args);
        inject(&mut extra, "--publication", &Some("seed_pub".to_owned()), &self.args);

        match name.as_str() {
            "bigquery" => {
                inject_env(&mut extra, "--bq-project-id", "TESTS_BIGQUERY_PROJECT_ID", &self.args);
                inject_env(
                    &mut extra,
                    "--bq-sa-key-file",
                    "TESTS_BIGQUERY_SA_KEY_PATH",
                    &self.args,
                );
            }
            "clickhouse" => {
                inject_env(&mut extra, "--clickhouse-url", "TESTS_CLICKHOUSE_URL", &self.args);
                inject_env(&mut extra, "--clickhouse-user", "TESTS_CLICKHOUSE_USER", &self.args);
                inject_env(
                    &mut extra,
                    "--clickhouse-password",
                    "TESTS_CLICKHOUSE_PASSWORD",
                    &self.args,
                );
            }
            "snowflake" => {}
            _ => {}
        }

        extra.extend(self.args.iter().cloned());

        let sh = Shell::new()?;
        let feature = name;
        cmd!(sh, "cargo run --bin {name} -p etl-examples --features {feature} -- {extra...}")
            .run()?;

        Ok(())
    }
}

fn env_or(key: &str, default: Option<&str>) -> Option<String> {
    std::env::var(key).ok().or_else(|| default.map(String::from))
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag || a.starts_with(&format!("{flag}=")))
}

fn inject_env(extra: &mut Vec<String>, flag: &str, env_var: &str, user_args: &[String]) {
    inject(extra, flag, &env_or(env_var, None), user_args);
}

fn inject(extra: &mut Vec<String>, flag: &str, value: &Option<String>, user_args: &[String]) {
    if has_flag(user_args, flag) {
        return;
    }
    if let Some(v) = value {
        extra.push(flag.to_owned());
        extra.push(v.clone());
    }
}
