# Build stage with cargo-chef for better layer caching.
# Native build: each runner builds for its own architecture.
FROM rust:1.93.1-slim-bookworm AS chef
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.72 --locked

# Fetch DuckDB extensions during the Docker build so they do not need to live
# in the repository or the local build context.
FROM debian:bookworm-slim AS duckdb-extensions
ARG TARGETPLATFORM
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    curl \
    gzip && \
    rm -rf /var/lib/apt/lists/*
COPY scripts/vendor_duckdb_extensions.sh /usr/local/bin/vendor_duckdb_extensions.sh
RUN chmod +x /usr/local/bin/vendor_duckdb_extensions.sh && \
    case "$TARGETPLATFORM" in \
      "linux/amd64") DUCKDB_PLATFORM="linux_amd64" ;; \
      "linux/arm64") DUCKDB_PLATFORM="linux_arm64" ;; \
      *) echo "unsupported duckdb extension platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    /usr/local/bin/vendor_duckdb_extensions.sh \
      --output-root /duckdb_extensions \
      --platform "$DUCKDB_PLATFORM"

# Install system dependencies
FROM chef AS planner
COPY . .
# xtask is a local dev tool and must not be part of the Docker build.
# Remove it from the workspace so cargo-chef / cargo metadata ignores it.
RUN sed -i '/"xtask"/d' Cargo.toml && rm -rf xtask
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies.
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Native build on $BUILDPLATFORM for $TARGETPLATFORM"

# Install build dependencies including lld linker for faster linking.
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client removes it from its deps.
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    lld \
    make && \
    rm -rf /var/lib/apt/lists/*

# Copy cargo config for build optimizations (lld linker, etc.).
COPY .cargo/config.toml /app/.cargo/config.toml

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application with optimizations.
# The release profile in Cargo.toml handles: thin LTO, strip, opt-level=3.
# ENABLE_EGRESS: set to "true" to enable egress metrics logging.
ARG ENABLE_EGRESS="false"
COPY . .
RUN sed -i '/"xtask"/d' Cargo.toml && rm -rf xtask
RUN FEATURES=""; \
    if [ "$ENABLE_EGRESS" = "true" ]; then FEATURES="--features egress"; fi && \
    RUSTFLAGS="-C panic=abort" cargo build --release -p etl-replicator $FEATURES && \
    strip target/release/etl-replicator

# Runtime stage with distroless for security.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user).
USER nonroot:nonroot

# jemalloc configuration is compiled into the binary (see main.rs).
# Use MALLOC_CONF env var to override at runtime if needed.
# Example: MALLOC_CONF="narenas:4,dirty_decay_ms:5000"

# Copy binary; configuration must be provided at runtime via mounted volume or environment overrides.
COPY --from=builder /app/target/release/etl-replicator ./etl-replicator
COPY --from=duckdb-extensions /duckdb_extensions ./duckdb_extensions

# Use exec form for proper signal handling.
ENTRYPOINT ["./etl-replicator"]
