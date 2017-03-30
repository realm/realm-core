FROM ubuntu:16.04

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcovr \
    git \
    g++-4.9 \
    libprocps4-dev \
    libssl-dev \
    ninja-build \
    pandoc \
    python-cheetah \
    python-matplotlib \
    python-pip \
    pkg-config \
    s3cmd \
    tar \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install diff_cover

RUN cd /opt \
    && wget https://cmake.org/files/v3.7/cmake-3.7.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.7.2-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.7.2-Linux-x86_64/bin"

VOLUME /source
VOLUME /out

WORKDIR /source
