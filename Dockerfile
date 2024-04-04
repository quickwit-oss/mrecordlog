# --- Builder

FROM rust:bullseye AS bin-builder

COPY . /mrecordlog

WORKDIR /mrecordlog/mrecordlog_cli

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/mrecordlog/target \
    cargo build --release --bin mrecordlog && \
    ls /mrecordlog/target/release && \
    mkdir -p /quickwit/bin && \
    mv /mrecordlog/target/release/mrecordlog /quickwit/bin/mrecordlog

# --- ACTUAL image.

FROM debian:bullseye-slim AS quickwit

LABEL org.opencontainers.image.title="Quickwit MRecordlog utils CLI"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

WORKDIR /quickwit

COPY --from=bin-builder /quickwit/bin/mrecordlog /usr/local/bin/mrecordlog

ENTRYPOINT ["mrecordlog"]
