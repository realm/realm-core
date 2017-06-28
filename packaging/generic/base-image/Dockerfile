FROM ubuntu:16.04

ARG PACKAGECLOUD_URL

RUN apt-get update && apt-get install -y \
  build-essential \
  g++-4.9 \
  git \
  libssl-dev \
  ninja-build \
  tar \
  wget \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN cd /opt \
    && wget https://cmake.org/files/v3.7/cmake-3.7.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.7.2-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.7.2-Linux-x86_64/bin"
