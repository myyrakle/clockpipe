FROM rust:1.89.0-alpine AS builder
COPY . /app 
WORKDIR /app
RUN apk add --no-cache musl-dev
RUN ln -sf /usr/bin/pkgconf /usr/bin/pkg-config
ENV PKG_CONFIG=/usr/bin/pkg-config
ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include
RUN cargo build --release

FROM alpine:3.22.1 AS deployer
# RUN apk add --no-cache ca-certificates libssl3
RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/target/release/clockpipe /app/clockpipe
# COPY clockpipe-config.json /app/config.json
ENTRYPOINT ["/app/clockpipe", "run", "--config-file", "/app/config.json"]
