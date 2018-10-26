FROM ubuntu:16.04

# Setup the LLVM repository
RUN echo deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-6.0 main > /etc/apt/sources.list.d/clang.list

# This forces dpkg not to call sync() after package extraction and speeds up install
RUN echo "force-unsafe-io" > /etc/dpkg/dpkg.cfg.d/02apt-speedup

# No need for the apt cache in a container
RUN echo "Acquire::http {No-Cache=True;};" > /etc/apt/apt.conf.d/no-cache

# Download the GBG key to use the LLVM repo
ADD https://apt.llvm.org/llvm-snapshot.gpg.key /tmp/llvm.key

# Add the key
RUN apt-key add /tmp/llvm.key

# Install clang and everything needed to build core
RUN apt-get update \
    && apt-get install -y clang-6.0 \
                       clang-format-6.0 \
                       cmake \
                       libprocps4-dev \
                       libssl-dev \
                       ninja-build \
                       git \
    && rm -rf /var/lib/apt/lists/*

# Make clang the default compiler
ENV CC /usr/bin/clang-6.0
ENV CXX /usr/bin/clang++-6.0
RUN ln -s /usr/bin/clang-format-6.0 /usr/bin/clang-format \
 && ln -s /usr/bin/git-clang-format-6.0 /usr/bin/git-clang-format
