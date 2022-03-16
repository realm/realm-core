# https://github.com/realm/ci/tree/master/realm/docker/build-ubuntu
FROM ghcr.io/realm/ci/build-env-ubuntu:pr-319

ENV CC gcc-11
ENV CXX g++-11

VOLUME /source
VOLUME /out

WORKDIR /source
