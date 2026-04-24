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
  - `xtask/`: workspace automation commands.
- Docs live in `docs/`.
- Local development and ops tooling live in `scripts/` and `DEVELOPMENT.md`.
- Tests live next to code in `src/` or `tests/`.

## Commands
- Build everything:
  - `cargo build --workspace --all-targets --all-features`
- Format:
  - `./scripts/fmt`
- Check formatting:
  - `./scripts/fmt-check`
- Lint:
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Run unit tests (no Postgres required):
  - `cargo nextest run --workspace --all-features --lib`
- Run tests for one crate:
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
- Never create commits, push branches, open pull requests, or perform other git write actions unless the user explicitly instructs you to do so.
- Keep the workspace on the stable toolchain from `rust-toolchain.toml` for build, lint, and test commands; use the pinned nightly formatter only through `./scripts/fmt` and `./scripts/fmt-check`.
- Treat `Cargo.toml` workspace lints, `rustfmt.toml`, and compiler diagnostics as the source of truth for enforceable style and correctness rules. Prefer adding or tightening static checks over adding prose rules here.

## Rust Style
- This section is only for project-specific judgment that is not already covered by rustfmt, rustc, or Clippy.
- Prefer absolute crate imports for shared module items, for example `use crate::metrics::{PIPELINE_ID_LABEL, APP_TYPE_LABEL};`, instead of `use super::{...};`.
- When multiple files share constants, helpers, or a single entrypoint, prefer a module directory with `mod.rs`.
- Keep top-level binaries focused on orchestration; move implementation detail into helpers or modules.
- Prefer clear, boring code over clever abstractions.
- Prefer existing workspace patterns over introducing new local conventions.
- Default to private visibility and only widen when a real caller requires it.
- Prefer the narrowest working visibility in this order: private, `pub(super)`, `pub(crate)`, then `pub`.
- Use `pub` only for intentional crate APIs consumed by other crates, integration tests, examples, or documented user-facing entrypoints.
- Keep struct fields private by default; prefer constructors, accessors, and focused helper methods over exposing mutable fields.
- When a module is internal, tighten the module itself before leaving deep items `pub`.
- In `mod.rs` and other module roots, prefer private child modules plus selective `pub use` lists over `pub mod` or `pub use child::*` when building a facade API.
- Treat `pub use` as part of the public API contract: re-export only items you intentionally want callers to depend on, and avoid wildcard re-exports from internal modules.
- When a crate re-exports dependency types through its own facade, prefer referring to them via the crate facade (for example `crate::types::SchemaError`) inside that crate unless you are working at an explicit integration boundary or the item is not re-exported.
- After visibility changes, verify with `cargo rustc -p <crate> --all-features -- -W unreachable_pub` for the relevant target and then rerun the smallest relevant checks/tests.
- Keep log message prose lowercase; SQL fragments, identifiers, and external product names may keep their required casing.

## Error Handling And Panics
- Use typed errors and `Result` for recoverable failures.
- Prefer propagating errors with context instead of flattening to strings.
- Reserve panics for programmer errors or violated invariants.
- Use `debug_assert!` and `unreachable!` where they make internal invariants explicit, but prefer typed errors for runtime failures that can be triggered by external input or system state.
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
- Register `NotifyingStore::notify_on_*` and `TestDestinationWrapper::wait_for_*` handles before the producer can fire. These helpers only arm on updates that arrive *after* registration, so register the notifier first, then start the producer.

  ```rust
  let ready = store.notify_on_table_state_type(id, Ready).await;
  pipeline.start().await.unwrap();
  ready.notified().await;
  ```

### Integration Test Style
- Prefer one-way integration tests: perform the source-side writes first, wait for the expected notifications, then call `shutdown_and_wait()` immediately before assertions.
- Avoid asserting destination state while the pipeline is still running unless the test is specifically about in-flight behavior or recovery during active replication.
- For `TestDestinationWrapper`, prefer asserting against the cumulative event history from `get_events()`. Use `clear_events()` only when restarting the pipeline or when a test intentionally needs to discard earlier history and assert on a new phase in isolation.
- Use `get_events_deduped()` only for replay/idempotence scenarios where duplicate wrapper observations are expected and the test is asserting logical equivalence rather than delivery history.
- When asserting CDC event shapes, only expect combinations that PostgreSQL can actually emit for the table's replica identity mode. In particular, distinguish between `FULL`, primary-key identity, and `USING INDEX`, and remember that partial update rows only occur for update new-tuples.

## Review Checklist
- Code compiles for the changed target or workspace as appropriate.
- Code is formatted.
- Clippy passes for the changed target or workspace as appropriate, using the workspace lint configuration as the source of truth for enforceable style and correctness rules.
- Tests pass for the changed target or workspace as appropriate.
- Doctests pass for the changed target or workspace as appropriate.
- Docs and comments match the final behavior.
- New metrics, logs, and labels follow existing naming patterns.
