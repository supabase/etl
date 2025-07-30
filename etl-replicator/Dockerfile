# Build stage with cargo-chef for better layer caching
FROM lukemathwalker/cargo-chef:latest-rust-1.88.0-slim-bookworm AS chef
WORKDIR /app

# Install system dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies
FROM chef AS builder
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client remove it from its deps
RUN apt-get update && apt-get install -y pkg-config libssl-dev protobuf-compiler clang && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release -p replicator && \
    strip target/release/replicator

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# Copy binary and configuration
COPY --from=builder /app/target/release/replicator ./replicator
COPY --chown=nonroot:nonroot replicator/configuration ./configuration

# Use exec form for proper signal handling
ENTRYPOINT ["./replicator"]