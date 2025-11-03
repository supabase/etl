# Build stage with cargo-chef for better layer caching
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.88.0-slim-bookworm AS chef
WORKDIR /app

# Install system dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM"
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client remove it from its deps
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    lld \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    binutils-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETPLATFORM" in \
      "linux/amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported TARGETPLATFORM: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    rustup target add "$TARGET_TRIPLE" && \
    echo "$TARGET_TRIPLE" > /tmp/target-triple
COPY --from=planner /app/recipe.json recipe.json
RUN TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    cargo chef cook --release --target "$TARGET_TRIPLE" --recipe-path recipe.json

# Build application
COPY . .
RUN TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    mkdir -p target/release && \
    RUSTFLAGS="-C panic=abort" cargo build --release --target "$TARGET_TRIPLE" -p etl-replicator && \
    if [ "$TARGET_TRIPLE" = "aarch64-unknown-linux-gnu" ]; then \
      STRIP_TOOL="aarch64-linux-gnu-strip"; \
    else \
      STRIP_TOOL="strip"; \
    fi && \
    "$STRIP_TOOL" "target/${TARGET_TRIPLE}/release/etl-replicator" && \
    cp "target/${TARGET_TRIPLE}/release/etl-replicator" target/release/etl-replicator

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# Copy binary and configuration
COPY --from=builder /app/target/release/etl-replicator ./etl-replicator
COPY --chown=nonroot:nonroot etl-replicator/configuration ./configuration

# Use exec form for proper signal handling
ENTRYPOINT ["./etl-replicator"]
