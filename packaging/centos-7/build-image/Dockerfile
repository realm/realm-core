FROM alanfranz/fwd-centos-7:latest
MAINTAINER Sebastian Lauwers <sl@realm.io>

# Install EPEL & devtoolset
RUN yum -y install \
        epel-release \
        centos-release-scl-rh \
    && yum-config-manager --enable rhel-server-rhscl-7-rpms

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
RUN yum -y install python-cheetah which chrpath devtoolset-3-gcc \
    devtoolset-3-gcc-c++ openssl-devel \
    && yum -y remove gcc gcc-c++ \
    && yum clean all

ENV CC /opt/rh/devtoolset-3/root/usr/bin/gcc
ENV CXX /opt/rh/devtoolset-3/root/usr/bin/g++
ENV LD /opt/rh/devtoolset-3/root/usr/bin/g++
