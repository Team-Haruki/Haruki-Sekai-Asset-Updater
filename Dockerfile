FROM debian:trixie-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    clang \
    pkg-config \
    build-essential \
    libavcodec-dev \
    libavdevice-dev \
    libavformat-dev \
    libavutil-dev \
    libswresample-dev \
    libswscale-dev && \
    rm -rf /var/lib/apt/lists/*
ENV PATH=/root/.cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --default-toolchain stable
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
COPY src src
COPY tests tests
ARG HARUKI_PACKAGE_VERSION=""
RUN if [ -n "${HARUKI_PACKAGE_VERSION}" ]; then \
        package_version="${HARUKI_PACKAGE_VERSION#v}"; \
        sed -i "0,/^version = /s#^version = .*#version = \"${package_version}\"#" Cargo.toml; \
        sed -i "0,/^version = /s#^version = .*#version = \"${package_version}\"#" crates/assetstudio-ffi/Cargo.toml; \
        cargo generate-lockfile; \
    fi
RUN cargo build --release --locked \
    -p haruki-sekai-asset-updater \
    -p haruki-assetstudio-ffi \
    --features haruki-sekai-asset-updater/media-ffi

FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libxml2 \
    libavcodec61 \
    libavformat61 \
    libavutil59 \
    libswresample5 \
    libswscale8 \
    git \
    openssh-client && \
    rm -rf \
    /var/lib/apt/lists/* \
    /var/cache/debconf/* \
    /usr/share/doc/* \
    /usr/share/info/* \
    /usr/share/lintian/* \
    /usr/share/man/*

WORKDIR /app
COPY --from=builder /app/target/release/haruki-sekai-asset-updater /app/haruki-sekai-asset-updater
COPY --from=builder /app/target/release/assetstudio_ffi_worker /app/assetstudio_ffi_worker
RUN mkdir -p logs

ENV TZ=Asia/Shanghai \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/app/assetstudio_ffi_worker \
    HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY=0 \
    HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS=256 \
    HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
