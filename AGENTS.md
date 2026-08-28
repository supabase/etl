# Repository Guidelines

Postgres logical-replication engine. `etl` is the library, `etl-replicator` is
the standalone binary, and `etl-destinations` holds built-in destinations.
`etl-api` is an optional Kubernetes control plane; you do not need it to run
ETL. Product docs: https://supabase.github.io/etl/. Human walkthrough:
[DEVELOPMENT.md](DEVELOPMENT.md).

This file is for agents. Read it before changing code. Use it to **set up** the
repo, not only to implement in it.

## First-time setup

Do this on a fresh clone before tests, examples, or local services.

1. Toolchain: Rust **1.95.0** from `rust-toolchain.toml` (rustup uses it
   automatically), `psql`, Docker Compose, and [cargo-nextest](https://nexte.st).
2. SQLx CLI (migrations; `cargo x init` and `cargo x setup api` need it):

   ```bash
   cargo install --version 0.9.0-alpha.1 sqlx-cli --no-default-features --features rustls,postgres --locked
   ```

3. Start the local data plane (Postgres, ClickHouse, Iceberg catalog, migrations).
   This does **not** write app config or apply Kubernetes:

   ```bash
   cargo x init
   ```

4. Then configure only the service you need:

   ```bash
   cargo x setup replicator && cargo x seed && cargo x run replicator
   cargo x setup api && cargo x run api
   ```

5. Generated files in `crates/etl-api/configuration/` and
   `crates/etl-replicator/configuration/` are gitignored. Re-run with `--force`
   to replace them. Do not commit them or put real secrets in tracked files.

| Need | Do |
| --- | --- |
| API / OrbStack k8s | `kubectl` plus [OrbStack](https://orbstack.dev) Kubernetes; `cargo x setup api` applies `scripts/k8s/local/` |
| Existing Postgres, no Docker | `SKIP_DOCKER=1` and `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` |
| Test clusters only | `cargo xtask postgres start` (or `create`) |
| Ports and destination env | [DEVELOPMENT.md](DEVELOPMENT.md) |

Default stack: Postgres `localhost:5430` (`postgres`/`postgres`), replica
`6430`, ClickHouse `http://localhost:8123` (`etl`/`etl`).

## Commands

`cargo x` is the workspace task runner (`cargo x --help`). Stable toolchain for
build/lint/test; nightly formatter only via `cargo x fmt`.

```bash
cargo build --workspace --all-targets --all-features
cargo x fmt
cargo x fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --lib          # unit tests, no Postgres
cargo nextest run -p etl-config --all-features              # one crate
cargo nextest list --workspace --all-features
cargo test --doc --workspace --all-features                 # nextest has no doctests
cargo xtask nextest run                                     # full sharded suite (needs Postgres)
```

After Rust changes, when diagnostics fail, when workflow assumptions change, or
when the user asks: run the **smallest** relevant build/clippy/test and report
what ran. Skip expensive workspace checks for docs-only or YAML-only edits.

## Layout

| Path | Owns |
| --- | --- |
| `crates/etl/` | Replication behavior and replication-specific types |
| `crates/etl-postgres/` | Reusable Postgres primitives, source helpers, slots, ETL metadata SQL |
| `crates/etl-destinations/` | Destination implementations |
| `crates/etl-config/` | Config types and loading |
| `crates/etl-api/` | HTTP control plane |
| `crates/etl-replicator/` | Standalone replicator binary |
| `crates/etl-telemetry/` | Tracing and Prometheus |
| `crates/etl-examples/`, `crates/etl-benchmarks/` | Examples and benches |
| `crates/xtask/` | `cargo x` automation |
| `site/` | Next.js/Fumadocs site; docs in `site/content/docs/` |
| `scripts/docker/`, `scripts/k8s/local/` | Compose stack and API k8s manifests |
| `src/` / `tests/` | Tests next to code; crate integration tests in `tests/main.rs` |

Crate boundaries are for reuse across crates, not for organizing one domain.
Keep code in its owning crate unless another crate has a real dependency.
Module names should be domain concepts (`schema`, `data`, `event`, `store`,
`source`, `slots`), not `types`/`utils`/`common`.

From `etl`, expose the smallest useful setup surface: pipeline
config/building, destination and store traits, ETL schema/data/event types, and
dependency types callers must use to implement those traits. Keep worker
orchestration, Postgres codec internals, runtime plumbing, and store
implementation details private.

## Working here

- Keep diffs focused. Prefer small changes unless a broader refactor is
  justified. Match nearby code before adding a new pattern. Do not add
  dependencies unless the task needs them.
- Never commit, push, open PRs, or perform other git writes unless the user
  explicitly asks.
- `Cargo.toml` workspace lints, `rustfmt.toml`, and compiler diagnostics are
  the enforceable style source of truth. Prefer tightening static checks over
  adding prose rules here.
- Migrations already on `main` are immutable (SQL, comments, whitespace,
  filenames). Add a new file. Do not edit a migration that is not yet on `main`
  unless the user asks, including comments.

### Destination compatibility

| Situation | Do |
| --- | --- |
| Schema *permits* a value/event the destination might not preserve | API preflight **warning** via stable destination capabilities; do not reject the schema |
| ETL can faithfully build the destination request/wire form | Send it; rely on the destination’s native behavior and errors |
| ETL cannot build that form, or a structural/state invariant is broken | Typed local error at the encoding point; same path for copy, insert, and update |

Do not add destination runtime value-domain checks merely to predict reject,
coerce, round, truncate, clamp, or reinterpret.

## Migrations

Treat each committed migration as a durable deployment boundary. Prefer **one
transactional migration** for one semantic transition (schema, backfill, and
cleanup together). Split only when PostgreSQL or a real deployment constraint
requires a commit boundary (operation must commit before its result can be
used, expand/backfill/contract, multi-version compatibility, or a backfill
whose locking/runtime must be separate). Document that limitation.

Every intermediate state must be valid, restart-safe, and retryable. Make
ordering explicit. Comments should explain what changes, why, how existing rows
are handled, and non-obvious locking, performance, compatibility, recovery, or
rollback impact — not restate the SQL. For reversible migrations, make
dependent data compatible in `down` before dropping the schema it needs. Add
focused tests when classification, backfill, retry, or rollback is not obvious
from the SQL.

## Secrets

Treat the repo, every branch, commit, PR, and review comment as **public**.
Never put real secrets, credentials, tokens, private URLs/hostnames, customer
data, or production payloads in code, tests, docs, comments, examples, commits,
PR text, logs, or generated files.

- **Do** use fake placeholders (`example.com`, `127.0.0.1`, `placeholder-token`)
  or `<redacted>`. Keep secrets in env vars, ignored files, or secret managers.
- **Do not** add secret-like values to rustdoc, snapshots, fixtures, panics,
  errors, or assertions, even in tests.
- If a value looks real (env, local config, command output, non-public source):
  do not commit or quote it; stop and ask, without repeating the secret.
- Before a requested git write, scan the staged diff, message, branch, and PR
  text. On review, flag suspected secrets with file:line and do not reproduce
  the full value. If something is already exposed: stop, recommend rotation,
  and do not push or paste the diff. Say what you checked for leaks.

## Rust

Project-specific judgment only; rustfmt/clippy already own formatting and most
lints.

- Absolute crate imports for shared items (`use crate::metrics::{...}`), not
  `use super::{...}`.
- SQL keywords and identifiers lowercase unless quoting or an API requires
  casing.
- Flat `schema.rs` when the module has no children; `mod.rs` only when it owns
  children or a grouped entrypoint. Prefer a module directory when several
  files share constants, helpers, or one entrypoint.
- No compatibility facades or wildcard re-exports during refactors unless the
  facade is an intentional public API. Re-export from domain modules; `pub use`
  is the public contract (no `pub use child::*`).
- Binaries orchestrate; implementation lives in helpers/modules. Prefer clear,
  boring code. Prefer existing workspace patterns.
- Compound names: singular attributive modifiers (`event_batch`,
  `EventBatchMetadata`), not `events_batch`. Keep plurals when the noun is the
  head (`table_rows`, `write_events`). Preserve established external names.
- Item order: helpers before use; `struct`, inherent `impl`, then trait impls.
  In inherent impls: constructors, public methods, private helpers.
- Do not add `#[must_use]` unless the user asks. Rustdoc goes **above** all
  attributes on the item (`#[derive]`, `#[serde]`, `#[cfg_attr]`, macros).
- Visibility: private by default, then `pub(super)`, `pub(crate)`, `pub`. `pub`
  only for crate APIs, integration tests, examples, or user-facing entrypoints.
  Fields private; constructors/accessors over mutable public fields. Tighten an
  internal module before leaving deep items `pub`. Prefer private children plus
  selective `pub use` in `mod.rs`. Inside a crate, refer to re-exported
  dependency types via the domain module (`crate::schema::SchemaError`) unless
  you are at an integration boundary. After visibility changes: `cargo rustc -p
  <crate> --all-features -- -W unreachable_pub`, then the smallest relevant
  checks.
- Log message prose lowercase; SQL, identifiers, and product names may keep
  required casing.
- Prefer `From`/`TryFrom` over `as` when the conversion exists or was
  range-checked. Use `as` for intentionally lossy conversions or ones Rust does
  not expose (`u64` → `f64` metrics). `From` only when the mapping is
  infallible, semantically lossless, value-preserving, and obvious.
- Patch-style API updates: omitted fields preserve stored values, explicit
  `null` clears or resets defaults, non-null replaces. Converting a non-patch
  API config into an update config needs an explicit helper such as
  `from_api_config`, not `From` (absent optionals would become `Clear`).

## Errors, panics, parsers

Typed `Result` for recoverable failures. Wrap with `source: error` or
`.with_source(error)`; do not embed `{error}` or `error.to_string()` in the
message. Detail fields are owned context (operations, tables, IDs, SQL).
Preserve the source chain. Error text is sentence case and starts with an
uppercase letter (`thiserror` included).

- `etl-api` HTTP responses: never leak Postgres/SQLx/database errors; generic
  customer message, original error in the internal chain and logs.
- ETL Postgres and DuckDB: keep the chain for debugging, still avoid highly
  critical data.
- Panics only for programmer errors or broken invariants. `debug_assert!` /
  `unreachable!` for internal invariants; typed errors for external input or
  system state. Document `# Panics` only when the function can panic.

Parser entrypoints must not panic on malformed input: typed parse error,
`EtlError`, `None`, or an explicit failure. Unexpected EOF, bad tokens, invalid
bytes/ranges, and unsupported syntax are recoverable. Prefer `slice.get`,
`str::get`, iterators, or byte-slice parsing; convert `None` to the parser
error. `[]` only when the invariant is local (`bytes[index]` while
`index < bytes.len()`, or `chunks_exact`). Parse ASCII protocols as bytes so
non-ASCII cannot panic on a UTF-8 boundary. `expect`/`panic!`/`unreachable!` in
parsers only for programmer invariants already guaranteed; the message must
state that invariant. Add malformed-input tests: empty, unterminated
quotes/escapes, invalid UTF-8-adjacent text, non-ASCII in ASCII formats,
overflow/underflow, unsupported syntax.

## Unsafe and concurrency

Avoid `unsafe` unless necessary. Every `unsafe` block needs a preceding
`// SAFETY:` comment. Prefer ownership/borrowing over extra clones or interior
mutability. Name long-running async work. Dropping a `JoinHandle` detaches:
`abort()` best-effort background tasks (metrics reporters) when they should
stop now. Dropping a `JoinSet` aborts its tasks; do not `abort_all()` before
returning from a scope that owns the set — only while the set is retained.
Graceful shutdown and join only when tasks own state that must finish (DB
transactions, destination flushes, retry-sensitive replication). Do not build
elaborate shutdown channels for timer/poll/telemetry tasks whose state can be
discarded.

## Docs, metrics, logs

Document every item (public and private), stdlib-style, punctuated. Link
[`Type`] and [`Type::method`]. Public/module docs: fully qualified links
outside the module (`[`crate::store::PipelineStore`]`). Item docs may use short
links when the type is imported and central. Do not add imports only to shorten
links. No rustdoc examples in this repo. Comments explain why, live on the line
above, and end with `.`.

Metric names and label keys are constants. Share labels in the parent metrics
module. Register descriptions once (`Once` or equivalent). One
`spawn_*_metrics_task` per source plus one orchestration helper. Low-cardinality
labels unless high cardinality is operationally required.

Do not log passwords, secrets, tokens, sensitive request/response bodies, or
source cell/row values. Structural metadata is fine (table/column/type names,
counts, lengths, LSNs, IDs, operations). Production errors: `error = %err` or
`error = %error` (not `err =`, `source =`, or debug for the primary error).
Prefer `Display` (`%`) over `Debug` (`?`) unless the type is known not to
contain sensitive values. Table state: `table_state_type` (one) and
`table_state_types` (list). Sentry: wrap sensitive API route groups with the
sensitive scope marker and scrub bodies on marked events; do not duplicate path
matchers in the scrubber.

## Tests

Process-per-test via cargo-nextest. Full sharded suite: `cargo xtask nextest
run`. Single crate: `cargo nextest run`. Integration tests live in
`tests/main.rs`; filter with `-- module_name::`. Doctests: `cargo test --doc`.

`0 passed; 0 failed; 0 ignored; n filtered out` is a **failure** (nothing ran).
Confirm expected tests actually ran. Prefer `cargo nextest list` before filters
if unsure. Fix one crate with the narrowest tests first, then broaden.

In tests, prefer `unwrap()`/`unwrap_err()` over `expect()`. Use `expect` only
when it adds diagnostics the expression does not already give; put non-obvious
intent in a preceding comment. Prefer assertions without custom messages. A
message is justified only for non-sensitive runtime context the default output
lacks (table-driven case ids). Add or update tests when behavior changes.
Reuse `crates/etl/src/test_utils/` and nearby helpers before inventing setup.

Register `NotifyingStore::notify_on_*` and `TestDestinationWrapper::wait_for_*`
**before** the producer can fire (they only arm on later updates):

```rust
let ready = store.notify_on_table_state_type(id, Ready).await;
pipeline.start().await.unwrap();
ready.notified().await;
```

Need Postgres? `cargo xtask postgres create` (or `cargo x init`) and
`TESTS_DATABASE_HOST`. Debug with `ENABLE_TRACING=1` and a focused `RUST_LOG`,
for example
`RUST_LOG=etl::replication::apply=debug,etl_destinations::bigquery=debug`.

Integration tests: source writes first, wait for notifications, then
`shutdown_and_wait()` before assertions. Do not assert destination state while
the pipeline is running unless the test is about in-flight or recovery
behavior. For `TestDestinationWrapper`, assert cumulative `get_events()`;
`clear_events()` only on restart or a new isolated phase. CDC shapes must be
ones PostgreSQL can emit for that replica identity (`FULL`, PK, `USING INDEX`);
partial update rows only occur for update new-tuples.

## Before you finish

Changed target or workspace, as appropriate: compiles, `cargo x fmt`, Clippy
with workspace lints, tests, doctests. Docs and comments match behavior. New
metrics, logs, and labels follow existing naming.
