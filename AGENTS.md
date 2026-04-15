# Repository Guidelines

## Workspace Layout
- Root workspace crates:
  - `etl/`: core replication library.
  - `etl-api/`: HTTP API service.
  - `etl-replicator/`: standalone replicator binary.
  - `etl-postgres/`: Postgres integration.
  - `etl-destinations/`: destination implementations.
  - `etl-config/`: configuration types and loading.
  - `etl-telemetry/`: tracing and Prometheus setup.
  - `etl-examples/`: examples.
  - `etl-benchmarks/`: benchmarks.
- Docs live in `docs/`.
- Local development and ops tooling live in `scripts/` and `DEVELOPMENT.md`.
- Tests live next to code in `src/` or `tests/`.

## Commands
- Build everything:
  - `cargo build --workspace --all-targets --all-features`
- Format:
  - `cargo fmt --all`
- Lint:
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Run unit tests (no Postgres required):
  - `cargo nextest run --workspace --all-features --lib`
- Run unit tests for one crate:
  - `cargo nextest run -p etl-config --all-features`
- Run doctests (nextest does not support doctests):
  - `cargo test --doc --workspace --all-features`
- List tests:
  - `cargo nextest list --workspace --all-features`
- Run full test suite (requires Postgres clusters via `cargo xtask postgres start`):
  - `cargo xtask nextest run`

## Agent Workflow
- Keep changes focused on the issue being solved.
- Prefer small diffs unless a broader refactor is clearly justified.
- Before adding new patterns, inspect nearby code and follow the local style first.
- Do not add dependencies unless they are justified by the task.
- If you change workflow assumptions, build or test the smallest relevant target and report what actually ran.

## Rust Style
- Follow default Rust formatting and idioms.
- Naming:
  - crates: `kebab-case`.
  - modules/files/functions/variables: `snake_case`.
  - types/traits/enums: `CamelCase`.
  - constants/statics: `SCREAMING_SNAKE_CASE`.
- Prefer absolute crate imports for shared module items, for example `use crate::metrics::{PIPELINE_ID_LABEL, APP_TYPE_LABEL};`, instead of `use super::{...};`.
- When multiple files share constants, helpers, or a single entrypoint, prefer a module directory with `mod.rs`.
- Keep top-level binaries focused on orchestration; move implementation detail into helpers or modules.
- Prefer clear, boring code over clever abstractions.
- Prefer existing workspace patterns over introducing new local conventions.
- All logs should be strictly lowercase.

## Error Handling And Panics
- Use typed errors and `Result` for recoverable failures.
- Prefer propagating errors with context instead of flattening to strings.
- Reserve panics for programmer errors or violated invariants.
- Only document `# Panics` when a function can actually panic.

## Unsafe And Concurrency
- Avoid `unsafe` unless it is necessary.
- Every `unsafe` block should have a preceding `// SAFETY:` comment explaining why it is sound.
- Prefer explicit ownership and borrowing over unnecessary cloning or interior mutability.
- In async code, keep long-running background work behind named tasks or helpers.

## Documentation
- Document all items, public and private, using concise stdlib-style prose.
- Link types and methods as [`Type`] and [`Type::method`].
- Keep comments and docs precise, short, and punctuated.
- Normal comments should always end with `.`.
- Do not add code examples in rustdoc for this repository.

## Metrics And Observability
- Define metric names and label keys as constants instead of inlining string literals at call sites.
- Centralize shared metric labels or tag values in the parent metrics module when multiple metric files use them.
- Register metric descriptions once using `std::sync::Once` or another one-time initialization pattern.
- For background metric polling, prefer one `spawn_*_metrics_task` helper per source and one orchestration helper that starts them.
- Prefer low-cardinality labels unless higher-cardinality labels are operationally necessary.

## Testing
- Tests run via `cargo-nextest` (process-per-test). Use `cargo xtask nextest run` for the full sharded suite, or `cargo nextest run` for single-crate runs.
- Integration tests are consolidated into `tests/main.rs` per crate. Address individual modules with `-- module_name::`.
- Doctests use `cargo test --doc` (nextest does not support them).
- If test output shows `0 passed; 0 failed; 0 ignored; n filtered out`, treat that as a failure to run tests.
- Verify that expected tests actually ran, not just that Cargo exited successfully.
- Prefer running `cargo nextest list` before using filters or crate-specific commands if there is any doubt.
- When fixing a specific crate, run the narrowest relevant tests first, then broaden if needed.
- Add or update tests when behavior changes, regressions are possible, or new logic is introduced.

## Review Checklist
- Code compiles for the changed target or workspace as appropriate.
- Code is formatted.
- Clippy passes for the changed target or workspace as appropriate.
- Tests pass for the changed target or workspace as appropriate.
- Doctests pass for the changed target or workspace as appropriate.
- Docs and comments match the final behavior.
- New metrics, logs, and labels follow existing naming patterns.
