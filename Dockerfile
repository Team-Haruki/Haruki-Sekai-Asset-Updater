FROM rust:1.95-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY src src
COPY tests tests
RUN cargo build --release

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
RUN cd AssetStudio/AssetStudioCLI && \
    dotnet publish -c Release -r linux-x64 -f net9.0 --self-contained true -o /app/assetstudio \
    -p:PublishTrimmed=false \
    -p:PublishSingleFile=true \
    -p:IncludeNativeLibrariesForSelfExtract=true
RUN cd AssetStudio/AssetStudioNative && \
    dotnet publish -c Release -r linux-x64 -f net9.0 --self-contained true -o /app/assetstudio-native \
    -p:TargetFrameworks=net9.0 \
    -p:PublishAot=true \
    -p:InvariantGlobalization=false && \
    cp /app/assetstudio-native/HarukiAssetStudioNative.so /app/assetstudio/ && \
    cp /app/assetstudio-native/libTexture2DDecoderNative.so /app/assetstudio/

FROM mwader/static-ffmpeg:8.1.1 AS ffmpeg-builder

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libicu76 \
    libxml2 \
    git \
    gnupg \
    openssh-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/haruki-sekai-asset-updater /app/haruki-sekai-asset-updater
COPY --from=assetstudio-builder /app/assetstudio /app/assetstudio
COPY --from=ffmpeg-builder /ffmpeg /usr/local/bin/ffmpeg
RUN ln -sf /app/assetstudio/AssetStudioModCLI /app/assetstudio/AssetStudioCLI && \
    mkdir -p logs

ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    HARUKI_ASSET_STUDIO_BACKEND=cli \
    HARUKI_ASSET_STUDIO_CLI_PATH=/app/assetstudio/AssetStudioCLI \
    HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH=/app/assetstudio/HarukiAssetStudioNative.so \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
