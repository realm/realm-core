FROM debian:stable

RUN echo "deb http://ftp.debian.org/debian stretch-backports main" > /etc/apt/sources.list.d/backports.list
RUN dpkg --add-architecture armhf && apt-get update
RUN apt-get install -y \
        build-essential \
        crossbuild-essential-armhf \
        libprocps-dev:armhf \
        libssl-dev:armhf \
        ninja-build \
        wget

RUN cd /opt \
    && wget https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.15.2-Linux-x86_64.tar.gz
ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
