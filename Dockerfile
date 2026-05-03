FROM rust:slim AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY . .
RUN cargo build --release --locked --bin biotechware-bot

FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/biotechware-bot .
COPY pricing.toml /app/pricing.toml

ENTRYPOINT ["./biotechware-bot"]
