FROM rust:1.90-alpine AS builder

# Determine Rust target based on build platform
ARG TARGETARCH
ARG TARGETPLATFORM
RUN case ${TARGETARCH} in \
    amd64) RUST_TARGET=x86_64-unknown-linux-musl ;; \
    arm64) RUST_TARGET=aarch64-unknown-linux-musl ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    echo "Building for ${RUST_TARGET}" && \
    rustup target add ${RUST_TARGET}

RUN apk update && \
    apk add --no-cache build-base musl-dev pkgconfig ca-certificates openssl-dev openssl-libs-static

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr

# Build with the determined target
ARG TARGETARCH
RUN case ${TARGETARCH} in \
    amd64) RUST_TARGET=x86_64-unknown-linux-musl ;; \
    arm64) RUST_TARGET=aarch64-unknown-linux-musl ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    cargo build --release --features server --target ${RUST_TARGET} && \
    cp /app/target/${RUST_TARGET}/release/plcbundle /app/plcbundle

FROM alpine:3 AS runtime
RUN adduser -D -u 10001 appuser && \
    apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates

WORKDIR /data

# Copy the binary from the fixed location in builder stage
COPY --from=builder --chmod=755 /app/plcbundle /usr/local/bin/plcbundle
USER appuser
EXPOSE 8080
ENTRYPOINT ["plcbundle"]
