# Development Guide

How to work in this repository. Coding agents should follow
[AGENTS.md](AGENTS.md) (first-time setup and implementation rules).

The open-source project is the replication engine: the `etl` library, the
`etl-replicator` binary, and the built-in destinations. Product docs live at
[supabase.github.io/etl](https://supabase.github.io/etl/).

## Start here

```bash
cargo x init
```

That starts local Postgres, ClickHouse, and the Iceberg REST catalog, and runs
migrations. It does not write service configuration or apply Kubernetes
resources.

Then configure the replicator:

```bash
cargo x setup replicator
cargo x seed
cargo x run replicator
```

Generated files in `crates/etl-replicator/configuration/` are gitignored.
Re-run setup with `--force` to replace them. Do not commit generated configuration.

Need: Rust from `rust-toolchain.toml`, `psql`, SQLx CLI, and Docker Compose.
The standalone replicator does not require Kubernetes. `cargo x deploy-local`
uses `kubectl` and an OrbStack Kubernetes cluster for optional local deployment.

Install SQLx CLI:

```bash
cargo install --version 0.9.0 sqlx-cli --no-default-features --features rustls,postgres --locked
```

## Replicator

ClickHouse is the default and uses the Docker service from `cargo x init`. No
cloud credentials are required. Iceberg REST also uses local Docker. Cloud
destinations write fake identifiers and placeholder secrets; override them with
`APP_DESTINATION__*` for a real run.

| Destination | Setup | Then |
| --- | --- | --- |
| `clickhouse` (default) | `cargo x setup replicator` | `cargo x seed && cargo x run replicator` |
| `iceberg` | `cargo x setup replicator --destination iceberg` | `cargo x seed && cargo x run replicator` |
| `bigquery` | `cargo x setup replicator --destination bigquery` | export `APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY`, then `cargo x run replicator` |
| `ducklake` | `cargo x setup replicator --destination ducklake` | export `APP_DESTINATION__DUCKLAKE__S3_*`, then `cargo x run replicator` |
| `snowflake` | `cargo x setup replicator --destination snowflake` | export `APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY`, then `cargo x run replicator` |

Iceberg is deprecated. For a Supabase catalog, add `--iceberg-catalog supabase`
and export `APP_DESTINATION__ICEBERG__SUPABASE__*`.

`cargo x setup replicator --help` lists flags. `--interactive` prompts for hosts
and names. `cargo x run replicator` compiles the destination from the generated
config. The replicator does not need Kubernetes. Re-run `cargo x seed` as often
as you like; it keeps the existing database unless you pass `--force`.

When the replicator is running, seed tables show up in ClickHouse as
`public_users`, `public_orders`, and `public_events`, plus `__current` views:

```bash
curl -sS 'http://localhost:8123/?user=etl&password=etl' \
  --data-binary 'SELECT count() FROM "public_users__current"'
```

Stop the replicator with Ctrl+C.

## Everyday commands

`cargo x` is the task runner. `cargo x --help` lists every command.

```bash
cargo x fmt              # nightly rustfmt (pinned)
cargo x fmt --check
cargo x check            # fmt, sort, clippy
cargo x fix
cargo x msrv             # verify MSRV consistency
cargo x migrate          # source and state-store migrations
cargo x deploy-local \
  --cpu-request 125m \
  --memory-request 250Mi # deploy replicator to local OrbStack k8s
cargo x test-clickhouse  # run ClickHouse integration tests
cargo x test-snowflake   # run Snowflake tests
cargo x vendor-duckdb    # download and vendor DuckDB extensions
cargo xtask nextest run  # full sharded test suite (needs Postgres)
```

## Local stack

`cargo x init` defaults:

| Service | Address |
| --- | --- |
| Postgres | `localhost:5430` (`postgres` / `postgres`) |
| Postgres replica | `localhost:6430` |
| ClickHouse | `http://localhost:8123` (`etl` / `etl`) |
| Iceberg catalog | `http://localhost:8182` |
| MinIO | `http://localhost:9010` (`minio-admin` / `minio-admin-password`) |

Override with `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`, `CLICKHOUSE_*`, or `SKIP_DOCKER=1` when
Postgres is already running. Persistent volume paths: `POSTGRES_DATA_VOLUME`,
`POSTGRES_REPLICA_DATA_VOLUME`, `CLICKHOUSE_DATA_VOLUME`.

`cargo xtask postgres start` starts only the test Postgres clusters.
`cargo xtask multigres --help` covers the optional Multigres cluster.

## Migrations

SQL is grouped by its owning subsystem:

| Directory | Purpose |
| --- | --- |
| `crates/etl/migrations/source/` | Helpers required in replication source databases. |
| `crates/etl/migrations/postgres_store/` | Durable state for `PostgresStore`. |
| `crates/etl-maintenance/migrations/postgres/` | External-maintenance coordination state. |

`cargo x migrate` creates the local database if needed and applies the first two
sets; `cargo x init` also runs it. Maintenance initialization belongs to its store.
See [replication migrations](crates/etl/migrations/README.md) and
[maintenance migrations](crates/etl-maintenance/migrations/README.md) for runtime
entrypoints, shared migration history, compatibility, and focused tests.

## Configuration

The replicator loads `configuration/base.yaml`, then
`configuration/{environment}.yaml`, then `APP_` environment variables (nested
keys use `__`). `APP_ENVIRONMENT` defaults to `prod`. `cargo x run` sets `dev`
and points `APP_CONFIG_DIR` at the generated directory.

Generated files include only required fields. Local Docker passwords are the
published Compose defaults. Cloud destination secrets are fake placeholders.

## Tests

After `cargo x init`:

```bash
cargo xtask nextest run
```

Unit tests that do not need Postgres:

```bash
cargo nextest run --workspace --all-features --lib
```

Integration tests need `TESTS_DATABASE_HOST`, `TESTS_DATABASE_PORT`, and
`TESTS_DATABASE_USERNAME` (local defaults: `localhost`, `5430`, `postgres`).
ClickHouse tests also need `TESTS_CLICKHOUSE_URL`, `TESTS_CLICKHOUSE_USER`, and
`TESTS_CLICKHOUSE_PASSWORD`. Iceberg tests use the local catalog and MinIO from
`cargo x init`. BigQuery tests need `TESTS_BIGQUERY_PROJECT_ID` and
`TESTS_BIGQUERY_SA_KEY_PATH`.

Debug a failing test with `ENABLE_TRACING=1` and a focused `RUST_LOG`.
Parser fuzz targets live in `fuzz/`.

Use cargo-nextest 0.9.133 to match CI; sharding requires at least 0.9.127.
CI builds one nextest archive for the Postgres compatibility matrix and
Multigres; coverage uses a separate instrumented build. Every Postgres and
OrioleDB lane runs the full regular suite, including destination tests and
BigQuery integration tests when credentials are available. Credentialed
Snowflake tests and Multigres tests run separately. Postgres shards start
concurrently and must all pass readiness checks before tests run. Nextest
distributes Postgres-backed tests across those clusters in round-robin slices;
tests sharing a cluster remain serial. BigQuery destination-only tests use
isolated datasets and an in-memory store. They run serially in the non-Postgres
lane, with priority over short unit tests to overlap remote work. BigQuery
pipeline tests remain on the Postgres shards. This scheduling is the same
locally and in every compatibility and coverage job. To reuse a local build:

```bash
mkdir -p target/ci
cargo nextest archive --locked --workspace --all-features --archive-file target/ci/tests.tar.zst
cargo --locked xtask nextest run --archive-file target/ci/tests.tar.zst
cargo --locked xtask multigres test --archive-file target/ci/tests.tar.zst
```

Archive consumers must use compatible operating systems and architectures.
CI sets `CARGO_INCREMENTAL=0` and `CARGO_PROFILE_DEV_DEBUG=0`; use both locally
when reproducing CI to reuse the same build profile instead of recompiling
dependencies with local debug settings.
Coverage: `cargo --locked xtask nextest llvm-cov`, followed by
`cargo llvm-cov report --locked --lcov --output-path target/ci/lcov.info`.

Check workflow syntax with `actionlint`. Build the production image on the
Docker daemon's native architecture:

```bash
docker buildx build --load --build-arg ENABLE_EGRESS=true \
  -f crates/etl-replicator/Dockerfile -t etl-replicator:local .
```

When a workspace dependency disables default features, consumers that need them
request `features = ["default"]`. This also lets cargo-chef preserve the feature
selection without modifying its generated recipe.

CI builds AMD64 and ARM64 on separate native workers, pushes each image by
digest, and verifies the combined manifest before tagging it at
`public.ecr.aws/supabase/etl-replicator`. Both builds check out the same resolved
source SHA. CI image checks and publishing share the build action and persistent
Blacksmith layer cache. Pulling these images requires no private registry access. Only a merged-main PR at the current main tip can promote
`latest`; manual builds never do. Require `CI passed` in branch protection
after its first run, replacing old required job names including `Snowflake Gate`.

Run **Publish image** with **Use workflow from: main** to use the current CI.
Set optional `commit` to a full 40-character lowercase SHA available in this
repository; leave it empty to build the selected workflow branch/tag's commit.
The source supplies the Dockerfile and application; publishing actions come from
the selected workflow revision, so older sources need not contain the new CI.

Every manual build publishes only `<full-sha>-experimental`, including rebuilds
of commits already on `main`. Only a PR merged into `main` publishes the plain
SHA tag, and only its current tip can update `latest`. Both architectures always
build the same resolved commit; there is no manual tag-mode override.

With `COMMIT` set to the desired source SHA:

```bash
gh workflow run publish-image.yml --repo supabase/etl --ref main \
  -f commit="$COMMIT"
```

## Documentation

The docs site uses Next.js and Fumadocs. Use Node.js 22, matching CI. From the
repository root:

```bash
cd site
npm ci
npm run dev
```

Open [http://localhost:3000/etl/](http://localhost:3000/etl/). Pages live in
`site/content/docs/` and reload as you edit them. Keep `/etl/` in the URL;
the root path returns 404. Use `localhost` to avoid development origin errors
with the network address printed by Next.js.

To check the exported site, run these commands from `site/`:

```bash
npm run build
npx playwright install chromium
npm run smoke:docs
```

For a manual preview of the build, run `npm run preview` and open the URL
printed by the server.

## Troubleshooting

- Nothing listens on 5430: run `cargo x init` (or `SKIP_DOCKER=1` with
  `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and
  `POSTGRES_DB`).
- Replicator config missing: `cargo x setup replicator` (ClickHouse) or
  `--destination <name>`.
- `cargo x seed` keeps `etl_testdata` if it already exists. Recreate with
  `cargo x seed --force`.
- Replicator fails with `replication slot "supabase_etl_apply_1" was not
  created in this database`: another pipeline used id `1` on this Postgres
  cluster (for example the first-pipeline tutorial). `cargo x setup
  replicator` drops an inactive leftover slot. If the slot is still active,
  stop that process, then re-run setup.

### Blacksmith caching

Image builds use native 8-vCPU AMD64 and ARM64 runners. Each Dockerfile and
architecture has one persistent BuildKit cache shared by CI and publication;
changing a workflow or commit does not create another cache. Dependency layers
stay ahead of application source, and package-manager cache mounts retain downloads
when a lockfile changes. Do not add `cache-from`/`cache-to` exports to these builds.

Regular dependency caches use upstream actions, which Blacksmith accelerates
automatically. Rust checks keep separate caches for compilation modes (Clippy,
tests, and coverage); only successful main-branch jobs save dependency caches.
Formatting, workflow lint, and publication orchestration use 2-vCPU runners;
Clippy and precompiled test execution use 4-vCPU runners; test archive builds
and coverage use 8-vCPU runners. Enable Blacksmith's **Branch Protection
for sticky disks** so pull requests can read trusted Docker caches without updating
the snapshots used for publication. Main-branch CI warms those snapshots.

The first build for a new cache key is cold. Check cache hits and build duration in
Blacksmith before increasing runner sizes; local rebuild timings do not measure
Blacksmith performance. See [Docker caching](https://docs.blacksmith.sh/blacksmith-caching/docker-builds),
[dependency caching](https://docs.blacksmith.sh/blacksmith-caching/dependencies-actions),
and [sticky disk protection](https://docs.blacksmith.sh/blacksmith-caching/dependencies-sticky-disks).
