FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y adb \
                       build-essential \
                       curl \
                       gcovr \
                       git \
                       gcc-7 \
                       g++-7 \
                       lcov \
                       libuv1-dev \
                       libprocps-dev \
                       ninja-build \
                       tar \
                       wget \
                       xutils-dev \
                       zlib1g-dev \
    && apt-get clean             

# Ensure a new enough version of CMake is available.
RUN cd /opt \
    && wget https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
        && tar zxvf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
