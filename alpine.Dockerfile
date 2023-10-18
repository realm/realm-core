FROM alpine:3.11

RUN apk add --no-cache --update \
    build-base \
    cmake \
    curl-dev \
    git \
    libuv-dev \
    ninja \
    openssl-dev \
    zlib-dev
