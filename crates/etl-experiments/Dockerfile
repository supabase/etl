# syntax=docker/dockerfile:1
FROM rust:1.95.0-slim-trixie AS builder
ENV RUSTUP_TOOLCHAIN=1.95.0
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends clang cmake make pkg-config && rm -rf /var/lib/apt/lists/*
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    find crates -type f -exec touch {} + && \
    cargo build --locked --release -p etl-experiments && \
    cp target/release/etl-experiments /etl-experiments

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tar && rm -rf /var/lib/apt/lists/*
COPY --from=builder /etl-experiments /usr/local/bin/etl-experiments
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/etl-experiments"]
