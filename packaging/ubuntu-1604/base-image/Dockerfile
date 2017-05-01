FROM ubuntu:16.04

ARG PACKAGECLOUD_URL

RUN apt-get update -y && apt-get install -y \
    build-essential \
    curl \
    ruby \
    ruby-dev \
    && gem install fpm

RUN curl -s $PACKAGECLOUD_URL/script.deb.sh | bash
