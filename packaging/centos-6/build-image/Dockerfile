FROM alanfranz/fwd-centos-6:latest
MAINTAINER Sebastian Lauwers <sl@realm.io>

# Install EPEL & devtoolset
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y install epel-release centos-release-scl-rh \
    && yum-config-manager --enable rhel-server-rhscl-6-rpms

# whatever is required for building should be installed in this image; just
# like BuildRequires: for RPM specs
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y install python-cheetah which chrpath devtoolset-3-gcc devtoolset-3-gcc-c++ \
    && yum -y remove gcc gcc-c++ \
    && yum clean all

ENV CC /opt/rh/devtoolset-3/root/usr/bin/gcc
ENV CXX /opt/rh/devtoolset-3/root/usr/bin/g++
ENV LD /opt/rh/devtoolset-3/root/usr/bin/g++
