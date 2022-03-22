FROM ubuntu:21.04

ARG CMAKE_VERSION=3.22.2

# This forces dpkg not to call sync() after package extraction and speeds up install
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup
# No need for the apt cache in a container
RUN echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-cache

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    curl \
    gcovr \
    gdb \
    git \
    gcc-11 \
    g++-11 \
    lcov \
    libcurl4-openssl-dev \
    libuv1-dev \
    libprocps-dev \
    ninja-build \
    pkg-config \
    python3-matplotlib \
    s3cmd \
    tar \
    unzip \
    valgrind \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p ~/.ssh \
 && ssh-keyscan -H github.com >> ~/.ssh/known_hosts

# Install CMake
RUN cd /opt \
    && curl -LO https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && tar zxvf cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && rm -f cmake-$CMAKE_VERSION-linux-x86_64.tar.gz
ENV PATH "/opt/cmake-$CMAKE_VERSION-linux-x86_64/bin:$PATH"

ENV CC gcc-11
ENV CXX g++-11

VOLUME /source
VOLUME /out

WORKDIR /source
