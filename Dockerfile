# -------- Build Stage --------
FROM rust:1.86.0-slim AS builder

WORKDIR /app
COPY . .

# Install musl tools for static linking
RUN apt-get update && apt-get install -y musl-tools pkg-config libssl-dev && \
    rustup target add x86_64-unknown-linux-musl

# Build release binary statically linked
RUN cargo build --release --target x86_64-unknown-linux-musl

# -------- Runtime Stage --------
FROM alpine:3.19

WORKDIR /app

# Copy just the binary from build
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/memtuco /usr/local/bin/memtuco

EXPOSE 7878

ENTRYPOINT ["memtuco"]
    