FROM ubuntu:18.04

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    curl \
    gcovr \
    git \
    gcc-7 \
    g++-7 \
    lcov \
    libprocps-dev \
    ninja-build \
    python-matplotlib \
    pkg-config \
    s3cmd \
    tar \
    unzip \
    valgrind \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"

VOLUME /source
VOLUME /out

WORKDIR /source
