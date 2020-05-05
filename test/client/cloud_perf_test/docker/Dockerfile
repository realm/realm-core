FROM ubuntu:bionic

WORKDIR /work

# Install prerequisites
COPY install_core_prerequisites_on_ubuntu_bionic.sh install_sync_prerequisites_on_ubuntu_bionic.sh /work/
RUN apt-get update && apt-get install -y "build-essential" "cmake" "ninja-build" && sh "install_core_prerequisites_on_ubuntu_bionic.sh" && sh "install_sync_prerequisites_on_ubuntu_bionic.sh"

# Build core library
COPY tmp/realm-core.tgz /work/
RUN mkdir "realm-core" && (cd "realm-core" && tar xf "../realm-core.tgz" && mkdir "build.release" && (cd "build.release" && cmake -G "Ninja" -D REALM_NO_TESTS="1" -D CMAKE_BUILD_TYPE="Release" .. && ninja) && mkdir "build.debug" && (cd "build.debug" && cmake -G "Ninja" -D REALM_NO_TESTS="1" -D CMAKE_BUILD_TYPE="Debug" .. && ninja))

# Build sync library and test client
COPY tmp/realm-sync.tgz /work/
RUN mkdir "realm-sync" && (cd "realm-sync" && tar xf "../realm-sync.tgz" && mkdir "build.release" && (cd "build.release" && cmake -G "Ninja" -D OPENSSL_ROOT_DIR="/usr" -D REALM_CORE_BUILDTREE="/work/realm-core/build.release" -D CMAKE_BUILD_TYPE="Release" .. && ninja && cp "test/test-client" "/usr/bin/realm-test-client") && mkdir "build.debug" && (cd "build.debug" && cmake -G "Ninja" -D OPENSSL_ROOT_DIR="/usr" -D REALM_CORE_BUILDTREE="/work/realm-core/build.debug" -D CMAKE_BUILD_TYPE="Debug" .. && ninja && cp "test/test-client" "/usr/bin/realm-test-client-dbg"))

WORKDIR /client
RUN rm -fr "/work" && mkdir -p "realms"

ENTRYPOINT ["realm-test-client"]
