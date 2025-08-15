FROM rust:1.89.0-alpine AS builder
COPY . /app 
WORKDIR /app
RUN apk add musl-dev
RUN cargo build --release
RUN ls -lah /app/target/release

FROM alpine:3.22.1 AS deployer
RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/target/release/clockpipe /app/clockpipe
RUN ls -lah /app
# COPY clockpipe-config.json /app/config.json
ENTRYPOINT ["/app/clockpipe", "run", "--config-file", "/app/config.json"]