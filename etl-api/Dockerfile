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
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies.
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Native build on $BUILDPLATFORM for $TARGETPLATFORM"

# Install build dependencies including lld linker for faster linking.
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    libssl-dev \
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
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release -p etl-api && \
    strip target/release/etl-api

# Runtime stage with distroless for security.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user).
USER nonroot:nonroot

# Copy binary; configuration must be provided at runtime via mounted volume or environment overrides.
COPY --from=builder /app/target/release/etl-api ./etl-api
COPY --from=duckdb-extensions /duckdb_extensions ./duckdb_extensions

# Use exec form for proper signal handling.
ENTRYPOINT ["./etl-api"]
