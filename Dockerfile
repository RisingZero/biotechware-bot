# syntax=docker/dockerfile:1.7
FROM rust:latest-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pre-cache deps
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && \
    cargo build --release && rm -rf src target/release/biotechware-bot*

COPY . .
RUN cargo build --release --locked

FROM debian:latest-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/biotechware-bot /usr/local/bin/biotechware-bot
COPY pricing.toml /app/pricing.toml
ENV PRICING_FILE=/app/pricing.toml
ENV METRICS_ADDR=0.0.0.0:9090
EXPOSE 9090
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/biotechware-bot"]
