FROM ci/realm-core:centos-6-base

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y install \
        git \
        wget \
        unzip \
        which \
        chrpath \
        openssl-devel \
        devtoolset-6-gcc \
        devtoolset-6-gcc-c++ \
        devtoolset-6-binutils \
    && yum clean all

# Install CMake
RUN cd /opt \
    && wget https://cmake.org/files/v3.7/cmake-3.7.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.7.2-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.7.2-Linux-x86_64/bin"

# Install ninja
RUN wget https://github.com/ninja-build/ninja/releases/download/v1.7.2/ninja-linux.zip \
    && unzip ninja-linux.zip \
    && chmod a+x ninja \
    && mv ninja /usr/bin

ENTRYPOINT ["/bin/bash", "/inside/docker-entrypoint.sh"]
CMD ["/bin/bash", "/inside/build-package"]
