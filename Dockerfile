FROM rust:1.80 as builder

COPY . /usr/src/exon
WORKDIR /usr/src/exon

ARG CARGO_BUILD_PROFILE=release

RUN if [ "$CARGO_BUILD_PROFILE" = "release" ]; \
    then cargo build --release --bin exon-cli; \
    else cargo build --bin exon-cli; \
    fi

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl-dev libdeflate-dev ca-certificates procps && rm -rf /var/lib/apt/lists/*
ARG CARGO_BUILD_PROFILE=release

COPY --from=builder /usr/src/exon/target/$CARGO_BUILD_PROFILE/exon-cli /usr/local/bin/exon-cli
