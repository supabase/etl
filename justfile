build:
	cargo build --workspace --all-targets --all-features

fmt:
	cargo fmt

lint:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo nextest run --all-features

test-nobigquery:
	cargo nextest run --all-features --profile no-bigquery

install-tools:
	cargo install cargo-nextest --locked
	cargo install just --locked
