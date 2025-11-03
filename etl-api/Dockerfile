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
RUN dpkg --add-architecture arm64 && \
    apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    clang \
    lld \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    binutils-aarch64-linux-gnu && \
    if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
      apt-get install -y --no-install-recommends \
        libssl-dev:arm64 \
        zlib1g-dev:arm64; \
    fi && \
    rm -rf /var/lib/apt/lists/*
RUN case "$TARGETPLATFORM" in \
      "linux/amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported TARGETPLATFORM: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    rustup target add "$TARGET_TRIPLE" && \
    echo "$TARGET_TRIPLE" > /tmp/target-triple
COPY --from=planner /app/recipe.json recipe.json
RUN TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    if [ "$TARGET_TRIPLE" = "aarch64-unknown-linux-gnu" ]; then \
      export PKG_CONFIG_ALLOW_CROSS=1 \
        PKG_CONFIG_LIBDIR=/usr/lib/aarch64-linux-gnu/pkgconfig:/usr/share/pkgconfig \
        PKG_CONFIG_SYSROOT_DIR=/usr/aarch64-linux-gnu \
        OPENSSL_DIR=/usr/aarch64-linux-gnu \
        OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu \
        OPENSSL_INCLUDE_DIR=/usr/include/aarch64-linux-gnu; \
    fi && \
    cargo chef cook --release --target "$TARGET_TRIPLE" --recipe-path recipe.json

# Build application
COPY . .
ENV SQLX_OFFLINE=true
RUN TARGET_TRIPLE=$(cat /tmp/target-triple) && \
    mkdir -p target/release && \
    if [ "$TARGET_TRIPLE" = "aarch64-unknown-linux-gnu" ]; then \
      export PKG_CONFIG_ALLOW_CROSS=1 \
        PKG_CONFIG_LIBDIR=/usr/lib/aarch64-linux-gnu/pkgconfig:/usr/share/pkgconfig \
        PKG_CONFIG_SYSROOT_DIR=/usr/aarch64-linux-gnu \
        OPENSSL_DIR=/usr/aarch64-linux-gnu \
        OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu \
        OPENSSL_INCLUDE_DIR=/usr/include/aarch64-linux-gnu; \
    fi && \
    cargo build --release --target "$TARGET_TRIPLE" -p etl-api && \
    if [ "$TARGET_TRIPLE" = "aarch64-unknown-linux-gnu" ]; then \
      STRIP_TOOL="aarch64-linux-gnu-strip"; \
    else \
      STRIP_TOOL="strip"; \
    fi && \
    "$STRIP_TOOL" "target/${TARGET_TRIPLE}/release/etl-api" && \
    cp "target/${TARGET_TRIPLE}/release/etl-api" target/release/etl-api

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# Copy binary and configuration
COPY --from=builder /app/target/release/etl-api ./etl-api
COPY --chown=nonroot:nonroot etl-api/configuration ./configuration

# Use exec form for proper signal handling
ENTRYPOINT ["./etl-api"]
