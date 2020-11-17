FROM ubuntu:18.04

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    curl \
    gcovr \
    gdb \
    git \
    gcc-8 \
    g++-8 \
    lcov \
    libcurl4-openssl-dev \
    libuv1-dev \
    libprocps-dev \
    ninja-build \
    pkg-config \
    python-matplotlib \
    s3cmd \
    tar \
    unzip \
    valgrind \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p ~/.ssh \
 && ssh-keyscan -H github.com >> ~/.ssh/known_hosts

# Ensure a new enough version of CMake is available.
RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
ENV CC gcc-8
ENV CXX g++-8

VOLUME /source
VOLUME /out

WORKDIR /source
