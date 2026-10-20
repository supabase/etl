# syntax=docker/dockerfile:1

# Pull: docker pull public.ecr.aws/supabase/etl-replicator:<full-commit-sha>
# No authentication required; one tag supports AMD64 and ARM64.
# Experimental tags end in -experimental; see DEVELOPMENT.md for local builds.

ARG RUST_VERSION=1.95.0
# Fetch DuckDB extensions at build time instead of vendoring them in the repository.
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

FROM rust:${RUST_VERSION}-slim-bookworm AS builder
ARG RUST_VERSION
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG ENABLE_EGRESS="false"
# Use the image's installed toolchain without installing local lint components.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
ENV RUSTFLAGS="--cfg tokio_unstable -C panic=abort -C link-arg=-fuse-ld=lld"
WORKDIR /app
RUN test "$BUILDPLATFORM" = "$TARGETPLATFORM"

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

# Cargo's target directory lives in a persistent cache mount instead of a
# cached dependency layer. A layer is rebuilt in full whenever Cargo.lock
# changes, which recompiles the bundled DuckDB library (about ten minutes on
# ARM64) even when DuckDB did not change. In the mount, Cargo rebuilds only the
# crates a change affects. Mounts are not part of the image, so the binaries are
# copied out in the same step.
#
# Cargo judges workspace crates fresh by file mtime, and a cached `COPY . .`
# layer can restore sources older than another build's output in the shared
# mount. Touching the workspace sources makes every build recompile them, so
# an image never ships workspace code from a different commit. Third-party
# crates live outside `crates/` and stay cached.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    case "$ENABLE_EGRESS" in \
      true) features="egress" ;; \
      false) features="" ;; \
      *) echo "ENABLE_EGRESS must be true or false" >&2; exit 1 ;; \
    esac && \
    find crates -type f -exec touch {} + && \
    cargo build --locked --release --package etl-replicator --bins --features "$features" && \
    mkdir -p /app/bin && \
    cp target/release/etl-replicator target/release/etl-ducklake-maintenance /app/bin/ && \
    strip /app/bin/etl-replicator /app/bin/etl-ducklake-maintenance

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

USER nonroot:nonroot

# jemalloc configuration is compiled into the binary (see main.rs).
# Use MALLOC_CONF env var to override at runtime if needed.
# Example: MALLOC_CONF="narenas:4,dirty_decay_ms:5000"

COPY --from=builder /app/bin/etl-replicator ./etl-replicator
COPY --from=builder /app/bin/etl-ducklake-maintenance ./etl-ducklake-maintenance
COPY --from=duckdb-extensions /duckdb_extensions ./duckdb_extensions

ENTRYPOINT ["./etl-replicator"]
