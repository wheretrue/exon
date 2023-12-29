FROM rust:1.70 as builder

COPY . /usr/src/exon
WORKDIR /usr/src/exon

RUN cargo build --release --bin exon-cli

FROM debian:bullseye-slim

COPY --from=builder /usr/src/exon/target/release/exon-cli /usr/local/bin/exon-cli

ENTRYPOINT ["exon-cli"]
