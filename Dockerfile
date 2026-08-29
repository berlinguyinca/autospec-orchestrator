# syntax=docker/dockerfile:1.7
FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release --bin autospec-orchestrator --bin autospec-worker --bin autospecctl

FROM debian:bookworm-slim AS controller
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/autospec-orchestrator /usr/local/bin/autospec-orchestrator
COPY --from=builder /src/target/release/autospecctl /usr/local/bin/autospecctl
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/autospec-orchestrator"]

FROM debian:bookworm-slim AS worker
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git lvm2 docker.io \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/autospec-worker /usr/local/bin/autospec-worker
ENTRYPOINT ["/usr/local/bin/autospec-worker"]
