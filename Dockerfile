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
    pandoc \
    python-cheetah \
    python-matplotlib \
    python-pip \
    pkg-config \
    ruby \
    ruby-dev \
    s3cmd \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install diff_cover

VOLUME /source
VOLUME /out

WORKDIR /source
