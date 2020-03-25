FROM ubuntu:16.04

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcovr \
    git \
    g++-4.9 \
    lcov \
    libprocps4-dev \
    libssl-dev \
    ninja-build \
    pkg-config \
    python-matplotlib \
    s3cmd \
    tar \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN cd /opt \
    && wget https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"

VOLUME /source
VOLUME /out

WORKDIR /source
