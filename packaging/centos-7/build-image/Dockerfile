FROM ci/realm-core:centos-7-base

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
RUN yum -y install \
        chrpath \
        cmake3 \
        devtoolset-3-binutils \
        devtoolset-3-gcc \
        devtoolset-3-gcc-c++ \
        ninja-build \
        openssl-devel \
        procps-devel \
        which \
    && yum clean all

ENTRYPOINT ["/bin/bash", "/inside/docker-entrypoint.sh"]
CMD ["/bin/bash", "/inside/build-package"]
