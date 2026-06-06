FROM rust:1.96-bookworm AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    pkg-config \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswresample-dev \
    libswscale-dev && \
    rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY src src
COPY tests tests
RUN cargo build --release --features media-ffi

FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS assetstudio-builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    clang \
    zlib1g-dev \
    binutils && \
    rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --single-branch --branch codex/native-aot-ffi https://github.com/Team-Haruki/AssetStudio.git
# Force dependency projects away from their net472 targets during NativeAOT publish.
RUN cd AssetStudio/AssetStudioFFI && \
    dotnet publish -c Release -r linux-x64 -f net9.0 --self-contained true -o /app/assetstudio-native \
    -p:TargetFrameworks=net9.0 \
    -p:PublishAot=true \
    -p:InvariantGlobalization=false

FROM mwader/static-ffmpeg:8.1.1 AS ffmpeg-builder

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libicu76 \
    libxml2 \
    libavcodec61 \
    libavformat61 \
    libavutil59 \
    libswresample5 \
    libswscale8 \
    git \
    gnupg \
    openssh-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/haruki-sekai-asset-updater /app/haruki-sekai-asset-updater
COPY --from=builder /app/target/release/assetstudio_native_worker /app/assetstudio_native_worker
COPY --from=assetstudio-builder /app/assetstudio-native /app/assetstudio
COPY --from=ffmpeg-builder /ffmpeg /usr/local/bin/ffmpeg
RUN mkdir -p logs

ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH=/app/assetstudio/HarukiAssetStudioFFI.so \
    HARUKI_ASSET_STUDIO_NATIVE_WORKER_PATH=/app/assetstudio_native_worker \
    HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE=pool \
    HARUKI_ASSET_STUDIO_NATIVE_PROCESS_CONCURRENCY=3 \
    HARUKI_ASSET_STUDIO_NATIVE_WORKER_MAX_CALLS=256 \
    HARUKI_ASSET_STUDIO_NATIVE_READ_BATCH_SIZE=32 \
    HARUKI_ASSET_STUDIO_NATIVE_MAX_EXPORT_TASKS=4 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
