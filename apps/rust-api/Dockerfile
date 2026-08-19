FROM rust:1.83-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/prism-rust-api /usr/local/bin/server
EXPOSE 8084
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD wget -q -O- http://127.0.0.1:8084/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/server"]
