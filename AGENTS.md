# Repository Guidelines

## Project Structure & Modules
- Rust workspace (`Cargo.toml`) with crates: `etl/` (core), `etl-api/` (HTTP API), `etl-postgres/`, `etl-destinations/`, `etl-replicator/`, `etl-config/`, `etl-telemetry/`, `etl-examples/`, `etl-benchmarks/`.
- Docs in `docs/`; ops tooling in `scripts/` (Docker Compose, DB init, migrations).
- Tests live per crate (`src` unit tests, `tests` integration); benches in `etl-benchmarks/benches/`.

## Build and Test
- Build: `cargo build --workspace --all-targets --all-features`.
- Lint/format: `cargo fmt`; `cargo clippy --all-targets --all-features -- -D warnings`.
- Use `ENABLE_TRACING=1` when running integration tests to see the logs.
- Use `RUST_LOG=[log-level]` if you need to see the logs with a specific log level.

## Coding Style & Naming
- Rust 2024 edition; keep formatter clean and warnings denied.
- Naming: crates `kebab-case`; files/modules `snake_case`; types/traits `CamelCase`; fns/vars `snake_case`.
- Do not leave comments when you remove things.

## Rust Docs Style
- Document all items, public and private, using stdlib tone and precision.
- Only use a "# Panics" section when a function can panic.
- Link types and methods as [`Type`], [`Type::method`].
- Keep wording concise, correct, and punctuated; reword for clarity while preserving intent.
- No code examples; include private helpers for maintainers; apply to modules, types, traits, impls, and functions.
- Normal comments, should always finish with `.`.

## Rust Tests Execution
- If output shows "0 passed; 0 failed; 0 ignored; n filtered out", tests did not run; treat as failure.
- Common fixes: add features (`cargo test --all-features` or `--features <flag>`), pick the right target (`--lib`, `--bins`, `--tests`), verify integration vs unit layout, remove name filters, or run for the workspace (`cargo test --workspace`) or a specific crate.
- Always verify actual execution: passed count > 0, test names appear, and counts match expectations.
- Quick diagnostic: list available tests with `cargo test -- --list` before running.
