# Stage 1: Build
FROM ubuntu:24.04 AS builder

ARG FM_VERSION=590

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        curl \
        ca-certificates \
        nvidia-fabricmanager-dev-${FM_VERSION} \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

COPY Cargo.toml ./
COPY src/ src/
RUN cargo build --release

# Stage 2: Runtime
FROM ubuntu:24.04

COPY --from=builder /build/target/release/fmclient /usr/local/bin/fmclient
COPY --from=builder /usr/lib/x86_64-linux-gnu/libnvfm* /usr/lib/x86_64-linux-gnu/
RUN ldconfig

ENTRYPOINT ["/bin/bash"]
