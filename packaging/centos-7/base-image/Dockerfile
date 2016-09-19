FROM alanfranz/fwd-centos-7:latest

ARG PACKAGECLOUD_URL

# Install EPEL & devtoolset
RUN yum -y install \
        epel-release \
        centos-release-scl-rh \
    && yum-config-manager --enable rhel-server-rhscl-7-rpms

RUN curl -s $PACKAGECLOUD_URL/script.rpm.sh | bash
