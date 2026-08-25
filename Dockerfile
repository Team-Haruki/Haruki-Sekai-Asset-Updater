FROM debian:trixie-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
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
ENV PATH=/root/.cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --default-toolchain stable
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY src src
COPY tests tests

# The asset engine (unity-rs) is a Cargo git dependency. While that repository
# is private the fetch needs a credential:
#
#   docker build --secret id=gh_token,src=<file-holding-a-token> .
#
# The helper below reads the mounted secret only while a fetch is running, so
# the token never lands in an image layer -- what is stored is the helper
# script, which names the mount path rather than the token. With no secret
# mounted the helper prints nothing and git falls back to an anonymous fetch,
# which is all a public repository needs, so this stays correct either way.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN git config --global credential.helper \
    '!f() { test -s /run/secrets/gh_token || exit 0; echo username=x-access-token; echo "password=$(cat /run/secrets/gh_token)"; }; f'

ARG HARUKI_PACKAGE_VERSION=""
RUN --mount=type=secret,id=gh_token \
    if [ -n "${HARUKI_PACKAGE_VERSION}" ]; then \
        package_version="${HARUKI_PACKAGE_VERSION#v}"; \
        sed -i "0,/^version = /s#^version = .*#version = \"${package_version}\"#" Cargo.toml; \
        cargo generate-lockfile; \
    fi
RUN --mount=type=secret,id=gh_token \
    cargo build --release --locked --features media-ffi

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
RUN mkdir -p logs

ENV TZ=Asia/Shanghai \
    HARUKI_MEDIA_BACKEND=ffi \
    HARUKI_ASSET_STUDIO_READ_BATCH_SIZE=32 \
    HARUKI_CONFIG_PATH=/app/haruki-asset-configs.yaml

EXPOSE 8080

CMD ["./haruki-sekai-asset-updater"]
