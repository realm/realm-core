FROM ubuntu:20.04

ARG CMAKE_VERSION=3.22.2

# This forces dpkg not to call sync() after package extraction and speeds up install
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup
# No need for the apt cache in a container
RUN echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-cache

# Install clang and everything needed to build core
RUN apt-get update \
    && apt-get install -y \
       clang-12 \
       clang-format-12 \
       curl \
       libcurl4-openssl-dev \
       libprocps-dev \
       libuv1-dev \
       ninja-build \
       git \
       gnupg \
       zlib1g-dev

# Make clang the default compiler
ENV CC clang
ENV CXX clang++
RUN ln -s /usr/bin/clang-12 /usr/bin/clang \
 && ln -s /usr/bin/clang++-12 /usr/bin/clang++ \
 && ln -s /usr/bin/clang-format-12 /usr/bin/clang-format \
 && ln -s /usr/bin/git-clang-format-12 /usr/bin/git-clang-format

# Install CMake
RUN cd /opt \
    && curl -LO https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && tar zxvf cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && rm -f cmake-$CMAKE_VERSION-linux-x86_64.tar.gz
ENV PATH "/opt/cmake-$CMAKE_VERSION-linux-x86_64/bin:$PATH"
