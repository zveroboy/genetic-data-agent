FROM rust:latest as builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
COPY rust-ingestion-worker ./rust-ingestion-worker

WORKDIR /usr/src/app/rust-ingestion-worker
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/app/rust-ingestion-worker/target/release/rust-ingestion-worker /app/rust-ingestion-worker

ENV PATH="/app:${PATH}"
ENTRYPOINT ["/app/rust-ingestion-worker"]
