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

FROM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS assetstudio-builder
WORKDIR /src
RUN apk add --no-cache git && \
    git clone --depth 1 https://github.com/Team-Haruki/AssetStudio.git && \
    cd AssetStudio/AssetStudioCLI && \
    dotnet publish -c Release -r linux-musl-x64 -f net9.0 --self-contained false -o /app/assetstudio \
    -p:PublishTrimmed=false \
    -p:PublishSingleFile=false

FROM mcr.microsoft.com/dotnet/runtime:9.0-alpine

RUN apk --no-cache add \
    ca-certificates \
    tzdata \
    ffmpeg \
    libgdiplus \
    icu-libs
WORKDIR /app
COPY --from=builder /app/haruki-sekai-asset-updater .
COPY --from=assetstudio-builder /app/assetstudio /app/assetstudio
RUN mkdir -p logs
ENV TZ=Asia/Shanghai \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false \
    ASSET_STUDIO_CLI_PATH=/app/assetstudio/AssetStudioCLI

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
