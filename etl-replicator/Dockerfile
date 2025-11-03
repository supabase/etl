# syntax=docker/dockerfile:1.6

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
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then dpkg --add-architecture arm64; fi && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      pkg-config \
      libssl-dev \
      zlib1g-dev \
      protobuf-compiler \
      clang \
      lld \
      gcc-aarch64-linux-gnu \
      g++-aarch64-linux-gnu \
      libc6-dev-arm64-cross \
      binutils-aarch64-linux-gnu \
      $(if [ "$TARGETPLATFORM" = "linux/arm64" ]; then printf '%s ' "libssl-dev:arm64" "zlib1g-dev:arm64"; fi) && \
    rm -rf /var/lib/apt/lists/*
RUN case "$TARGETPLATFORM" in \
      "linux/amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported TARGETPLATFORM: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    rustup target add "$TARGET_TRIPLE" && \
    echo "$TARGET_TRIPLE" > /tmp/target-triple
RUN cat <<'EOF' >/usr/local/bin/with-cross-env
#!/usr/bin/env bash
set -euo pipefail
target="$(cat /tmp/target-triple 2>/dev/null || true)"
if [ "$target" = "aarch64-unknown-linux-gnu" ]; then
  export PKG_CONFIG_ALLOW_CROSS=1
  export PKG_CONFIG_LIBDIR=/usr/lib/aarch64-linux-gnu/pkgconfig:/usr/share/pkgconfig
  export PKG_CONFIG_SYSROOT_DIR=/usr/aarch64-linux-gnu
  export OPENSSL_DIR=/usr/aarch64-linux-gnu
  export OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu
  export OPENSSL_INCLUDE_DIR=/usr/include/aarch64-linux-gnu
fi
exec "$@"
EOF
RUN chmod +x /usr/local/bin/with-cross-env
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    with-cross-env cargo chef cook --release --target "$TARGET_TRIPLE" --locked --recipe-path recipe.json

# Build application
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    mkdir -p target/release && \
    RUSTFLAGS="-C panic=abort" with-cross-env cargo build --release --target "$TARGET_TRIPLE" --locked -p etl-replicator && \
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
