FROM alanfranz/fwd-centos-6:latest
MAINTAINER Sebastian Lauwers <sl@realm.io>

# Install EPEL & devtoolset
RUN yum -y install epel-release centos-release-scl-rh \
    && yum-config-manager --enable rhel-server-rhscl-6-rpms

# whatever is required for building should be installed in this image; just like BuildRequires: for RPM specs
RUN yum -y install python-cheetah which chrpath devtoolset-3-gcc devtoolset-3-gcc-c++ \
    && yum -y remove gcc gcc-c++

ENV CC /opt/rh/devtoolset-3/root/usr/bin/gcc
ENV CXX /opt/rh/devtoolset-3/root/usr/bin/g++
ENV LD /opt/rh/devtoolset-3/root/usr/bin/g++
