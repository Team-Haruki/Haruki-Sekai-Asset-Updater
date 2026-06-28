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
    -p:InvariantGlobalization=true

FROM mcr.microsoft.com/dotnet/sdk:8.0-bookworm-slim AS haruki-3d-exporter-builder
ARG HARUKI_3D_EXPORTER_REPOSITORY=https://github.com/storyxy3/Haruki-3D-Exporter.git
ARG HARUKI_3D_EXPORTER_BRANCH=main
ARG ASSETSTUDIO_REPOSITORY=https://github.com/Team-Haruki/AssetStudio.git
ARG ASSETSTUDIO_BRANCH=sekai-modified
ENV DEBIAN_FRONTEND=noninteractive \
    ASSETSTUDIO_REPOSITORY=${ASSETSTUDIO_REPOSITORY} \
    ASSETSTUDIO_BRANCH=${ASSETSTUDIO_BRANCH} \
    ASSETSTUDIO_ROOT=/src/AssetStudio
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    clang \
    zlib1g-dev \
    binutils && \
    rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --single-branch --branch "${ASSETSTUDIO_BRANCH}" "${ASSETSTUDIO_REPOSITORY}" AssetStudio
RUN git clone --depth 1 --single-branch --branch "${HARUKI_3D_EXPORTER_BRANCH}" "${HARUKI_3D_EXPORTER_REPOSITORY}" Haruki-3D-Exporter
RUN cd Haruki-3D-Exporter && \
    dotnet restore \
        -p:AssetStudioRoot="${ASSETSTUDIO_ROOT}" \
        -p:RestoreConfigFile=NuGet.Config && \
    dotnet publish -c Release -o /app/haruki-3d-exporter \
        --no-restore \
        -p:AssetStudioRoot="${ASSETSTUDIO_ROOT}"

FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim AS dotnet-runtime

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
COPY --from=assetstudio-builder /app/assetstudio-ffi /app/assetstudio
COPY --from=dotnet-runtime /usr/share/dotnet /usr/share/dotnet
COPY --from=dotnet-runtime /usr/bin/dotnet /usr/bin/dotnet
COPY --from=haruki-3d-exporter-builder /app/haruki-3d-exporter /app/bin/haruki-3d-exporter
RUN mkdir -p logs && \
    printf '#!/bin/sh\nexec dotnet /app/bin/haruki-3d-exporter/Haruki-3D-Exporter.dll "$@"\n' > /app/bin/Haruki-3D-Exporter && \
    chmod +x /app/bin/Haruki-3D-Exporter

ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=/app/assetstudio/HarukiAssetStudioFFI.so \
    HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/app/assetstudio_ffi_worker \
    HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY=0 \
    HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS=256 \
    HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
