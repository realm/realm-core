FROM ubuntu:16.04

ARG PACKAGECLOUD_URL

RUN apt-get update -y && apt-get install -y \
    curl \
    gcc \
    make \
    ruby \
    ruby-dev \
    && gem install fpm

RUN curl -s $PACKAGECLOUD_URL/script.deb.sh | bash
