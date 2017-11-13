FROM ci/realm-core:centos-7-base

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
RUN yum -y install \
        chrpath \
        devtoolset-3-binutils \
        devtoolset-3-gcc \
        devtoolset-3-gcc-c++ \
        ninja-build \
        openssl-devel \
        procps-devel \
        wget \
        which \
    && yum clean all

# Install CMake
RUN cd /opt \
    && wget https://cmake.org/files/v3.9/cmake-3.9.6-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.9.6-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.9.6-Linux-x86_64/bin"

ENTRYPOINT ["/bin/bash", "/inside/docker-entrypoint.sh"]
CMD ["/bin/bash", "/inside/build-package"]
