FROM golang:1.25.3-alpine3.22 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X haruki-sekai-asset/config.Version=${VERSION}" \
    -o haruki-sekai-asset-updater \
    -trimpath \
    -tags netgo \
    .

FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS assetstudio-builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --single-branch --branch sekai-modify https://github.com/Team-Haruki/AssetStudio.git
RUN cd AssetStudio/AssetStudioCLI && \
    dotnet publish -c Release -r linux-x64 -f net9.0 --self-contained true -o /app/assetstudio \
    -p:PublishTrimmed=false \
    -p:PublishSingleFile=true \
    -p:IncludeNativeLibrariesForSelfExtract=true

FROM mwader/static-ffmpeg:7.1.1 AS ffmpeg-builder

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libicu76 \
    libxml2 && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/haruki-sekai-asset-updater .
COPY --from=assetstudio-builder /app/assetstudio /app/assetstudio
COPY --from=ffmpeg-builder /ffmpeg /usr/local/bin/ffmpeg
RUN ln -sf /app/assetstudio/AssetStudioModCLI /app/assetstudio/AssetStudioCLI && \
    mkdir -p logs
ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    ASSET_STUDIO_CLI_PATH=/app/assetstudio/AssetStudioCLI

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
