# syntax=docker/dockerfile:1
# Build stage with cargo-chef for dependency layer caching.
# Native build: each runner builds for its own architecture.
ARG RUST_VERSION=1.95.0
FROM rust:${RUST_VERSION}-slim-bookworm AS chef
ARG RUST_VERSION
# Use the image's installed toolchain without installing local lint components.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.72 --locked

# Fetch DuckDB extensions during the Docker build so they do not need to live
# in the repository or the local build context.
FROM debian:bookworm-slim AS duckdb-extensions
ARG TARGETPLATFORM
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gzip && \
    rm -rf /var/lib/apt/lists/*
COPY scripts/bin/vendor-duckdb-extensions.sh /usr/local/bin/vendor-duckdb-extensions.sh
RUN chmod +x /usr/local/bin/vendor-duckdb-extensions.sh && \
    case "$TARGETPLATFORM" in \
      "linux/amd64") DUCKDB_PLATFORM="linux_amd64" ;; \
      "linux/arm64") DUCKDB_PLATFORM="linux_arm64" ;; \
      *) echo "unsupported duckdb extension platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    /usr/local/bin/vendor-duckdb-extensions.sh \
      --output-root /duckdb_extensions \
      --platform "$DUCKDB_PLATFORM"

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies.
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
# Select the same package, binaries, features, and flags in both build stages.
ARG ENABLE_EGRESS="false"
ENV RUSTFLAGS="-C panic=abort -C link-arg=-fuse-ld=lld"
RUN test "$BUILDPLATFORM" = "$TARGETPLATFORM"

# Install build dependencies including lld linker for faster linking.
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client removes it from its deps.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    lld \
    make && \
    rm -rf /var/lib/apt/lists/*

# Keep Cargo configuration identical during dependency and application builds.
COPY .cargo/config.toml /app/.cargo/config.toml

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    case "$ENABLE_EGRESS" in \
      true) features="egress" ;; \
      false) features="" ;; \
      *) echo "ENABLE_EGRESS must be true or false" >&2; exit 1 ;; \
    esac && \
    cargo chef cook --locked --release --package etl-replicator --bins \
      --features "$features" --recipe-path recipe.json

# Build both runtime binaries with thin LTO and opt-level=3 from Cargo.toml.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    case "$ENABLE_EGRESS" in \
      true) features="egress" ;; \
      false) features="" ;; \
      *) echo "ENABLE_EGRESS must be true or false" >&2; exit 1 ;; \
    esac && \
    cargo build --locked --release --package etl-replicator --bins --features "$features" && \
    strip target/release/etl-replicator target/release/etl-ducklake-maintenance

# Runtime stage with distroless for security.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

USER nonroot:nonroot

# jemalloc configuration is compiled into the binary (see main.rs).
# Use MALLOC_CONF env var to override at runtime if needed.
# Example: MALLOC_CONF="narenas:4,dirty_decay_ms:5000"

# Copy binary; configuration must be provided at runtime via mounted volume or environment overrides.
COPY --from=builder /app/target/release/etl-replicator ./etl-replicator
COPY --from=builder /app/target/release/etl-ducklake-maintenance ./etl-ducklake-maintenance
COPY --from=duckdb-extensions /duckdb_extensions ./duckdb_extensions

# Use exec form for proper signal handling.
ENTRYPOINT ["./etl-replicator"]
