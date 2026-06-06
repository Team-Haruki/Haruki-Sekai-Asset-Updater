FROM ubuntu:26.04 AS builder

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
COPY src src
COPY tests tests
RUN cargo build --release --locked --features media-ffi

FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS assetstudio-builder
ARG TARGETARCH
WORKDIR /src
ARG ASSETSTUDIO_REPOSITORY=https://github.com/Team-Haruki/AssetStudio.git
ARG ASSETSTUDIO_BRANCH=sekai-modified
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    clang \
    zlib1g-dev \
    binutils && \
    rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --single-branch --branch "${ASSETSTUDIO_BRANCH}" "${ASSETSTUDIO_REPOSITORY}" AssetStudio
# Force dependency projects away from their net472 targets during NativeAOT publish.
RUN cd AssetStudio/AssetStudioFFI && \
    case "${TARGETARCH}" in \
        amd64) runtime_id=linux-x64 ;; \
        arm64) runtime_id=linux-arm64 ;; \
        *) echo "Unsupported Docker target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    dotnet publish -c Release -r "${runtime_id}" -f net9.0 --self-contained true -o /app/assetstudio-ffi \
    -p:TargetFrameworks=net9.0 \
    -p:PublishAot=true \
    -p:InvariantGlobalization=false

FROM mwader/static-ffmpeg:8.1.1 AS ffmpeg-builder

FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libicu78 \
    libxml2-16 \
    libavcodec62 \
    libavdevice62 \
    libavformat62 \
    libavutil60 \
    libswresample6 \
    libswscale9 \
    git \
    gnupg \
    wget \
    openssh-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/haruki-sekai-asset-updater /app/haruki-sekai-asset-updater
COPY --from=builder /app/target/release/assetstudio_ffi_worker /app/assetstudio_ffi_worker
COPY --from=assetstudio-builder /app/assetstudio-ffi /app/assetstudio
COPY --from=ffmpeg-builder /ffmpeg /usr/local/bin/ffmpeg
RUN mkdir -p logs

ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=/app/assetstudio/HarukiAssetStudioFFI.so \
    HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/app/assetstudio_ffi_worker \
    HARUKI_ASSET_STUDIO_FFI_CALL_MODE=pool \
    HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY=0 \
    HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS=256 \
    HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
