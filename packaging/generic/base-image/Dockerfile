FROM centos:6

# Install EPEL & devtoolset
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
 && yum -y install \
      epel-release \
      centos-release-scl-rh \
 && yum-config-manager --enable rhel-server-rhscl-6-rpms \
 && yum clean all
