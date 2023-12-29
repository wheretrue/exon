FROM rust:1.70 as builder

COPY . /usr/src/exon
WORKDIR /usr/src/exon

RUN cargo build --bin exon-cli

FROM debian:bullseye-slim

COPY --from=builder /usr/src/exon/target/debug/exon-cli /usr/local/bin/exon-cli

ENTRYPOINT ["exon-cli"]
