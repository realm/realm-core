FROM ubuntu:20.04

# This forces dpkg not to call sync() after package extraction and speeds up install
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup

# No need for the apt cache in a container
RUN echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-cache

# Install clang and everything needed to build core
RUN apt-get update \
    && apt-get install -y \
       clang-11 \
       clang-format-11 \
       libcurl4-openssl-dev \
       libprocps-dev \
       libuv1-dev \
       ninja-build \
       git \
       gnupg \
       wget \
       zlib1g-dev

# Make clang the default compiler
ENV CC clang
ENV CXX clang++
RUN ln -s /usr/bin/clang-11 /usr/bin/clang \
 && ln -s /usr/bin/clang++-11 /usr/bin/clang++ \
 && ln -s /usr/bin/clang-format-11 /usr/bin/clang-format \
 && ln -s /usr/bin/git-clang-format-11 /usr/bin/git-clang-format

RUN cd /opt \
    && wget -nv https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.tar.gz \
    && tar zxf cmake-3.21.3-linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.21.3-linux-x86_64/bin:$PATH"
