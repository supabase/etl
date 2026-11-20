# Working on Supabase ETL

Replication belongs in `crates/etl`, reusable Postgres primitives in
`crates/etl-postgres`, and built-in destinations in `crates/etl-destinations`.
Keep the public `etl` API focused on configuring pipelines, implementing stores
and destinations, and consuming replicated data. Keep worker orchestration,
codecs, and store internals private. Public docs live in `site/content/docs`.

## Setup and verification

- For tools, local services, and port overrides, follow
  [Development](DEVELOPMENT.md#start-here). Install the Rust toolchain from
  `rust-toolchain.toml`, `psql`, Docker Compose, SQLx CLI, and cargo-nextest as
  needed. Run `cargo --locked x init` before running local examples or the
  replicator. Generated replicator config belongs in the ignored
  `crates/etl-replicator/configuration/` directory.
- For the sharded suite, run `cargo --locked xtask postgres start` and then
  `cargo --locked xtask nextest run`. See [Tests](DEVELOPMENT.md#tests) for
  credentials, TLS, and shard ports. Use disposable local test databases.
- After Rust changes, run the smallest relevant build, Clippy check with
  `-D warnings`, `cargo --locked x fmt --check`, and tests. Use
  `cargo nextest run --locked -p <crate> --all-features` for crate tests and
  `cargo test --locked --doc -p <crate> --all-features` for affected doctests.
  Check that the intended tests ran. For workflow or docs-only changes, use
  `actionlint` or the [docs checks](DEVELOPMENT.md#documentation) as relevant.
- Leave commits, pushes, PRs, and other Git writes to the user unless they
  explicitly ask. Treat every branch, PR, review comment, and fixture as public.
  Check the staged diff, branch name, and publication text for secrets before
  a requested Git write. Flag suspected secrets by location and omit the
  value. Stop and recommend rotation if a real credential is already exposed.

## Rust and dependencies

- Use domain modules and keep code in its owning crate unless another crate
  needs to reuse it. Put implementation in modules and keep binaries focused
  on orchestration. Use `schema.rs` for a leaf module and `mod.rs` for a
  grouped entry point.
- Import referenced items and use their short names. Use absolute `crate::`
  imports for shared items and import re-exported types from their domain
  modules. Keep fields private and export only the APIs callers need. Use
  named re-exports. After changing visibility, run
  `cargo rustc --locked -p <crate> --all-features -- -W unreachable_pub`.
- Order imports, constants, then other items. Put helpers before their users,
  and put constructors and public methods before private methods. Group
  setup, validation, work, error handling, and assertions with blank lines.
  Separate `select!` branches and leave a blank line after `biased;`.
  Check this grouping when reviewing the diff.
- Use the workspace lints and formatter. Use lowercase SQL and log prose
  where the protocol and identifiers permit. Use singular modifiers such as
  `event_batch`. Add punctuated rustdoc above item attributes, link related
  items, and use fully qualified links outside the module in public and
  module docs. Put runnable examples in the public guides or examples crate.
  Comments explain why. Add `#[must_use]` only on request.
- Use full `x.y.z` registry requirements with Cargo's default caret range.
  Document any tighter compatibility constraint next to the dependency. Put
  shared dependencies in `[workspace.dependencies]`, pin Git dependencies by
  `rev`, and include intentional lockfile changes. CI build, test, and lint
  commands use `--locked`, including Cargo commands spawned by `xtask`.
- For config integers, use the external or persisted domain's type and
  validate any narrower range. For unconstrained settings, use `u16` for
  small counts and `u64` for sizes, durations, and large limits. Use
  `NonZero*` where zero is invalid, with a primitive `utoipa` schema
  override. Reserve `usize` for memory sizes and APIs that require it. Use
  `From` for infallible lossless conversions and `TryFrom` for checked
  conversions. Saturate to `usize::MAX` only for ceilings.
- Patch config preserves omitted values, clears or resets on explicit
  `null`, and replaces non-null values. Use a named conversion from a full
  config so absent fields do not accidentally clear values.

## Replication contracts

- Keep committed migrations on `main` byte-for-byte and add new files.
  Edit an unmerged migration only on request. Prefer a single transaction
  per schema transition. If a commit boundary is required, document it and
  make each intermediate state valid and retryable. Cover locking, backfill
  and rollback behavior where it matters. In a down migration, make data
  compatible before dropping schema it depends on.
- Expose destination limitations as capabilities when a schema merely allows
  values a destination may coerce or reject. If ETL can encode a value, send
  it and use the destination's native behavior. Return a typed error at the
  shared copy/insert/update encoding point when representation is impossible
  or a structural invariant fails.

## Failures and diagnostics

- Use typed errors for recoverable failures. Preserve causes with `source`
  or `.with_source(error)` and put structural context in details. Start error
  messages with an uppercase letter. Reserve panics for programmer
  invariants and document `# Panics` when applicable.
- Parsers must handle malformed input without panicking. Use byte parsing for
  ASCII formats and checked indexing where the bounds are external. Cover
  malformed cases such as truncation, non-ASCII input, invalid escapes, and
  overflow. Use a preceding `// SAFETY:` for every `unsafe` block.
- Recover a poisoned sync lock only if the guarded state remains valid.
  Otherwise return a typed error, such as `InvalidState` for an unusable
  native DuckDB connection.
- Use fake data and placeholders in tracked files. Keep credentials, raw
  provider payloads, customer data, and private URLs out of all diagnostics,
  including error chains, panics, and Sentry. Expose secrets only at protocol
  or storage boundaries. Use `SecretString`, `SerializableSecretString` only
  when serialization is needed, and `Zeroizing` for temporary key material.
- Log primary failures as `error = %error` or `error = %err`. Use constants
  for metric names and label keys. Share keys in the parent metrics module,
  keep labels low-cardinality, and register descriptions once. Use
  `table_state_type` and `table_state_types` for table states.

## Task ownership and shutdown

- The spawner owns teardown unless it transfers the handle. Use
  `AbortOnDropHandle`, `TaskGroup` for fallible children, and `TaskRegistry`
  for shared destination tasks. Reap destination tasks during operation
  and retain table-sync results for the final wait.
- On controlled shutdown, stop intake and join children. Accept requested
  cancellations and propagate panics, task errors, and unexpected
  cancellations promptly. On failure, drop remaining handles to request
  abort. Keep the pipeline completion future alive while requesting
  shutdown. Use `with_shutdown!` only for cancellation-safe waits.
- Drain apply work within `shutdown_drain_timeout_ms` under the existing
  retry and durability policy. Never force an early flush or checkpoint
  unfinished work. At the deadline, cancel and join workers before
  destination teardown. With `drain_on_shutdown = false`, leave pending
  work for replay.
- Keep startup and copy waits cancellable, including the final copy write.
  Clean up constructed destinations on startup cancellation and join copy
  children before releasing their snapshot. Keep memory sampling alive
  through destination cleanup. Dispatch long CDC writes from `write_events`.
- Never block or spawn cleanup in `Drop`. Keep native-work guards and
  interruption active until started blocking work exits. Document the
  lifetime and termination of any detached task. See
  [Shutdown and replay validation](DEVELOPMENT.md#shutdown-and-replay-validation).

## Integration tests

- Reuse [ETL test helpers](crates/etl/src/test_utils/) and add focused
  regressions for behavior changes. Test lifecycle boundaries with channels
  or state barriers. Register `NotifyingStore::notify_on_*` and
  `TestDestinationWrapper::wait_for_*` before starting the producer.
- Write to the source, wait for notifications, then call
  `shutdown_and_wait()` before asserting destination state unless the test
  concerns in-flight behavior. Assert cumulative `get_events()`; clear
  events only on restart or between isolated phases. Use CDC shapes
  PostgreSQL can emit for the configured replica identity.
