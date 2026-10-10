# ── Build stages (cargo-chef: dependencies are cooked in their own cached layer) ──
FROM lukemathwalker/cargo-chef:0.1.78-rust-trixie AS chef

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

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
# The workspace members the root crate depends on. Without these the build
# fails at manifest resolution, before it ever reaches a source file.
COPY crates crates
COPY src src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --features media-ffi --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY src src

# The version comes from Cargo.toml (bumped before tagging; CI never passes this arg).
# For a manual build with another version string, only this package's own entries in
# Cargo.toml and Cargo.lock are rewritten: the dependency set stays the committed lock.
ARG HARUKI_PACKAGE_VERSION=""
RUN if [ -n "${HARUKI_PACKAGE_VERSION}" ]; then \
        package_version="${HARUKI_PACKAGE_VERSION#v}"; \
        sed -i "0,/^version = /s#^version = .*#version = \"${package_version}\"#" Cargo.toml; \
        sed -i "/^name = \"haruki-sekai-asset-updater\"$/{n;s#^version = .*#version = \"${package_version}\"#}" Cargo.lock; \
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

# The user and /app (owned by it) come first; the binary is copied with --chown. A `chown -R`
# after the COPY would duplicate the whole binary into a second layer.
RUN groupadd --gid 10001 haruki && \
    useradd --uid 10001 --gid haruki --no-create-home --home-dir /app \
      --shell /usr/sbin/nologin haruki && \
    install -d -o haruki -g haruki /app /app/logs

WORKDIR /app
COPY --from=builder --chown=10001:10001 /app/target/release/haruki-sekai-asset-updater /app/haruki-sekai-asset-updater

ENV TZ=Asia/Shanghai \
    MALLOC_ARENA_MAX=4 \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

USER haruki:haruki

CMD ["./haruki-sekai-asset-updater"]
