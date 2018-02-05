FROM alanfranz/fwd-centos-6:latest

ARG PACKAGECLOUD_URL

# Install EPEL & devtoolset
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
    && yum -y install \
        epel-release \
        centos-release-scl-rh \
        yum-utils \
    && yum-config-manager --enable rhel-server-rhscl-6-rpms \
    && curl -s $PACKAGECLOUD_URL/script.rpm.sh | bash \
    && yum clean all
