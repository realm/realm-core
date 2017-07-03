FROM ci/realm-core:centos-7-base

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
RUN yum -y install \
        cmake3 \
        ninja-build \
        which \
        chrpath \
        openssl-devel \
        devtoolset-3-gcc \
        devtoolset-3-gcc-c++ \
        devtoolset-3-binutils

ENTRYPOINT ["/bin/bash", "/inside/docker-entrypoint.sh"]
CMD ["/bin/bash", "/inside/build-package"]
