FROM rust:1.86-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY tests ./tests
COPY benches ./benches

RUN cargo build --release --bin server

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates curl \
	&& rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 daikon \
	&& useradd --uid 1000 --gid daikon --shell /bin/false daikon \
	&& mkdir -p /app/data /app/snapshots \
	&& chown -R daikon:daikon /app

COPY --from=builder /app/target/release/server /usr/local/bin/server

USER daikon

ENV KV_BIND=0.0.0.0:8080
ENV KV_STORE_PATH=/app/data/server_store.json
ENV KV_WAL_PATH=/app/data/server.wal
ENV KV_SNAPSHOTS_DIR=/app/snapshots

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
	CMD curl -sf http://localhost:8080/api/health/live || exit 1

CMD ["server"]