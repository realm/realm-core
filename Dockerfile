FROM ubuntu:16.04

# One dependency per line in alphabetical order.
# This should help avoiding duplicates and make the file easier to update.
RUN apt-get update && apt-get install -y \
    build-essential \
    g++-4.9 \
    libprocps4-dev \
    libssl-dev \
    pandoc \
    python-cheetah \
    pkg-config \
    ruby \
    ruby-dev \
    && rm -rf /var/lib/apt/lists/*

VOLUME /source
VOLUME /out

WORKDIR /source
