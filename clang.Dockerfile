FROM ubuntu:18.04

# This forces dpkg not to call sync() after package extraction and speeds up install
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup

# No need for the apt cache in a container
RUN echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-cache

# Install clang and everything needed to build core
RUN apt-get update \
    && apt-get install -y \
       libcurl4-openssl-dev \
       libprocps-dev \
       libuv1-dev \
       ninja-build \
       git \
       gnupg \
       wget \
       zlib1g-dev

# Setup the LLVM repository
RUN echo deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-11 main > /etc/apt/sources.list.d/clang.list
# Download the GBG key to use the LLVM repo
ADD https://apt.llvm.org/llvm-snapshot.gpg.key /tmp/llvm.key

# Add the key
RUN apt-key add /tmp/llvm.key

RUN apt-get update \
    && apt-get install -y clang-11 clang-format-11 \
    && rm -rf /var/lib/apt/lists/*

# Make clang the default compiler
ENV CC clang
ENV CXX clang++
RUN ln -s /usr/bin/clang-11 /usr/bin/clang \
 && ln -s /usr/bin/clang++-11 /usr/bin/clang++ \
 && ln -s /usr/bin/clang-format-11 /usr/bin/clang-format \
 && ln -s /usr/bin/git-clang-format-11 /usr/bin/git-clang-format

RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
