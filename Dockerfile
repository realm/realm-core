FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
  ruby ruby-dev build-essential g++-4.9 pkg-config libssl-dev python-cheetah \
  libprocps4-dev pandoc

VOLUME /source
VOLUME /out

WORKDIR /source
