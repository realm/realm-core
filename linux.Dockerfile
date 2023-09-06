# https://github.com/realm/ci/tree/master/realm/docker/build-ubuntu
FROM ghcr.io/realm/ci/build-env-ubuntu:master

VOLUME /source
VOLUME /out

WORKDIR /source
