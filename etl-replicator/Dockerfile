# Build stage with cargo-chef for better layer caching
# Native build: each runner builds for its own architecture
FROM lukemathwalker/cargo-chef:latest-rust-1.88.0-slim-bookworm AS chef
WORKDIR /app

# Install system dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies and compile
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Native build on $BUILDPLATFORM for $TARGETPLATFORM"

# Install build dependencies including lld linker for faster linking
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

# Copy cargo config for build optimizations (lld linker, etc.)
COPY .cargo/config.toml /app/.cargo/config.toml

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application with optimizations
# The release profile in Cargo.toml handles: thin LTO, debug symbols, opt-level=3
# --remap-path-prefix makes DWARF paths relative so source context works in CI
COPY . .
RUN RUSTFLAGS="-C panic=abort --remap-path-prefix=/app=." cargo build --release -p etl-replicator

# Strip stage - creates stripped binary and split debug companion
# Uses objcopy to create a proper debug file (smaller than full unstripped binary)
# Both files share the same GNU build-id for Sentry matching
FROM builder AS stripper
RUN apt-get update && apt-get install -y --no-install-recommends binutils && rm -rf /var/lib/apt/lists/* \
    && objcopy --only-keep-debug target/release/etl-replicator target/release/etl-replicator.debug \
    && strip --strip-unneeded target/release/etl-replicator \
    && objcopy --add-gnu-debuglink=target/release/etl-replicator.debug target/release/etl-replicator

# Debug symbols export stage - minimal stage containing only debug binary
# Used by CI to extract debug symbols without copying entire filesystem
FROM scratch AS debug-symbols
COPY --from=stripper /app/target/release/etl-replicator.debug /etl-replicator.debug
COPY --from=stripper /app/target/release/etl-replicator /etl-replicator

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# jemalloc configuration is compiled into the binary (see main.rs).
# Use MALLOC_CONF env var to override at runtime if needed.
# Example: MALLOC_CONF="narenas:4,dirty_decay_ms:5000"

# Copy stripped binary; configuration must be provided at runtime via mounted volume or environment overrides
COPY --from=stripper /app/target/release/etl-replicator ./etl-replicator

# Use exec form for proper signal handling
ENTRYPOINT ["./etl-replicator"]
