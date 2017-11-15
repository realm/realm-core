FROM centos:6

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y install \
        epel-release \
        centos-release-scl-rh \
    && yum -y install \
        chrpath \
        devtoolset-3-binutils \
        devtoolset-3-gcc \
        devtoolset-3-gcc-c++ \
        git \
        openssl-devel \
        procps-devel \
        unzip \
        wget \
        which \
    && yum clean all \
    && /usr/bin/scl enable devtoolset-3 true

# Install CMake
RUN cd /opt \
    && wget https://cmake.org/files/v3.9/cmake-3.9.6-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.9.6-Linux-x86_64.tar.gz \
    && rm -f cmake-3.9.6-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.9.6-Linux-x86_64/bin"

# Install ninja
RUN cd /opt \
    && wget https://github.com/ninja-build/ninja/releases/download/v1.7.2/ninja-linux.zip \
    && unzip ninja-linux.zip \
    && rm -f ninja-linux.zip \
    && mv ninja /usr/bin \
    && chmod a+rx /usr/bin/ninja

# Make sure the above SCLs are already enabled
ENTRYPOINT ["/usr/bin/scl", "enable", "devtoolset-3", "--"]
CMD ["/usr/bin/scl", "enable", "devtoolset-3", "--", "/bin/bash"]
