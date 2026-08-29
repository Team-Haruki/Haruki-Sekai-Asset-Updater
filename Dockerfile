FROM rust:trixie AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
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
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
# The workspace members the root crate depends on. Without these the build
# fails at manifest resolution, before it ever reaches a source file.
COPY crates crates
COPY src src
COPY tests tests

ARG HARUKI_PACKAGE_VERSION=""
RUN if [ -n "${HARUKI_PACKAGE_VERSION}" ]; then \
        package_version="${HARUKI_PACKAGE_VERSION#v}"; \
        sed -i "0,/^version = /s#^version = .*#version = \"${package_version}\"#" Cargo.toml; \
        cargo generate-lockfile; \
    fi
RUN cargo build --release --locked --features media-ffi

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
RUN groupadd --gid 10001 haruki && \
    useradd --uid 10001 --gid haruki --no-create-home --home-dir /app \
      --shell /usr/sbin/nologin haruki && \
    mkdir -p logs && \
    chown -R haruki:haruki /app

ENV TZ=Asia/Shanghai \
    MALLOC_ARENA_MAX=4 \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

USER haruki:haruki

CMD ["./haruki-sekai-asset-updater"]
