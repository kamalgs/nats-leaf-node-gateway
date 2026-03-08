# Stage 1: Build
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy manifests first for dependency caching.
COPY Cargo.toml Cargo.lock ./

# Create dummy source files so cargo can resolve the package
RUN mkdir -p src && \
    echo "fn main() {}" > src/lib.rs && \
    mkdir -p examples && \
    echo "fn main() {}" > examples/leaf_server.rs

# Build dependencies only (cached layer)
RUN cargo build --release --example leaf_server 2>/dev/null || true

# Copy actual source
COPY . .

# Touch sources to invalidate the dummy builds
RUN touch src/lib.rs examples/leaf_server.rs

# Build the real binary
RUN cargo build --release --example leaf_server

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/examples/leaf_server /usr/local/bin/leaf-server

EXPOSE 4222
EXPOSE 4223

ENTRYPOINT ["leaf-server"]
CMD ["--port", "4222"]
