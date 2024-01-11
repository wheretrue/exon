FROM rust:1.70 as builder

COPY . /usr/src/exon
WORKDIR /usr/src/exon

RUN cargo build --bin exon-cli

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y libssl-dev ca-certificates procps && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/exon/target/debug/exon-cli /usr/local/bin/exon-cli
