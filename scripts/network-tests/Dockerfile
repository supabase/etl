FROM postgres:18 AS database
RUN apt-get update && apt-get install -y --no-install-recommends \
    dnsmasq python3 iproute2 iptables && rm -rf /var/lib/apt/lists/*
COPY database.sh controller.py /lab/
ENTRYPOINT ["bash", "/lab/database.sh"]

FROM rust:1.95.0-slim-trixie AS tester
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl cmake make pkg-config libssl-dev \
    dnsutils iproute2 && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      arm64) rust_arch=aarch64; nextest_arch=linux-arm ;; \
      amd64) rust_arch=x86_64; nextest_arch=linux ;; \
      *) exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/v0.18.0/sccache-v0.18.0-${rust_arch}-unknown-linux-musl.tar.gz" \
      -o /tmp/sccache.tar.gz && \
    tar -xzf /tmp/sccache.tar.gz -C /tmp && \
    install "/tmp/sccache-v0.18.0-${rust_arch}-unknown-linux-musl/sccache" /usr/local/bin/sccache && \
    curl -fsSL "https://get.nexte.st/0.9.148/${nextest_arch}" -o /tmp/nextest.tar.gz && \
    tar -xzf /tmp/nextest.tar.gz -C /usr/local/cargo/bin && \
    rm -rf /tmp/sccache* /tmp/nextest.tar.gz
ENV RUSTUP_TOOLCHAIN=1.95.0 RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0 \
    SCCACHE_CACHE_SIZE=20G SCCACHE_DIR=/cache/sccache \
    CC="sccache cc" CXX="sccache c++" SQLX_OFFLINE=true
CMD ["sleep", "infinity"]
