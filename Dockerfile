FROM alanfranz/fwd-centos-6:latest

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y update \
    && yum -y install \
        epel-release \
        centos-release-scl-rh \
    && yum-config-manager --enable rhel-server-rhscl-6-rpms \
    && yum -y install \
        which \
        chrpath \
        openssl-devel \
        python-cheetah \
        devtoolset-3-gcc \
        devtoolset-3-gcc-c++ \
        devtoolset-3-binutils \
        lcov \
        libcurl-devel \
    && yum clean all

RUN curl -SL https://cmake.org/files/v3.5/cmake-3.5.2-Linux-x86_64.tar.gz | tar -xzC /usr --strip-components 1

RUN curl -SL https://github.com/gcovr/gcovr/archive/3.3.tar.gz | tar -zxC / \
    && cd /gcovr-3.3 \
    && python setup.py build \
    && python setup.py install \
    && cd / && rm -rf /gcovr-3.3

ENTRYPOINT ["scl", "enable", "devtoolset-3"]
