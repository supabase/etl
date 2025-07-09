# Build stage with cargo-chef for better layer caching
FROM lukemathwalker/cargo-chef:latest-rust-1.85.0-slim-bookworm AS chef
WORKDIR /app

# Install system dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies
FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release -p api && \
    strip target/release/api

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# Copy binary and configuration
COPY --from=builder /app/target/release/api ./api
COPY --chown=nonroot:nonroot api/configuration ./configuration

# Use exec form for proper signal handling
ENTRYPOINT ["./api"]
