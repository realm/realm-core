FROM ubuntu:16.04

ARG PACKAGECLOUD_URL

RUN apt-get update -y && apt-get install -y \
    build-essential \
    curl \
    git \
    ninja-build \
    ruby \
    ruby-dev \
    tar \
    wget \
    && gem install --no-ri --no-rdoc fpm

RUN cd /opt \
    && wget https://cmake.org/files/v3.7/cmake-3.7.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.7.2-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.7.2-Linux-x86_64/bin"

RUN curl -s $PACKAGECLOUD_URL/script.deb.sh | bash
