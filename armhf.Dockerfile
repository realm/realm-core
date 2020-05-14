FROM debian:10

RUN dpkg --add-architecture armhf && apt-get update
RUN apt-get install -y \
        build-essential \
        curl \
        crossbuild-essential-armhf \
        git \
        libprocps-dev:armhf \
        libssl-dev:armhf \
        libz-dev:armhf \
        libcurl4-openssl-dev:armhf \
        libasio-dev \
        ninja-build \
        qemu-user

RUN cd /opt \
    && curl -s -O -J https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.15.2-Linux-x86_64.tar.gz \
    && rm cmake-3.15.2-Linux-x86_64.tar.gz
ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
