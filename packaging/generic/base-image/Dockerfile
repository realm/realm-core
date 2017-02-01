FROM ubuntu:16.04

ARG PACKAGECLOUD_URL

RUN apt-get update && apt-get install -y \
  build-essential \
  cmake \
  g++-4.9 \
  git \
  libssl-dev \
  ninja-build \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

