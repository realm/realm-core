FROM centos:6

# Install EPEL & devtoolset
# On CentOS6, there is a bug with OverlayFS and Docker. It is needed to touch
# /var/lib/rpm/* in order to work around this issue.
# Link: https://github.com/docker/docker/issues/10180
RUN touch /var/lib/rpm/* \
 && yum -y install \
      epel-release \
      centos-release-scl-rh

RUN yum -y install \
      chrpath \
      devtoolset-6-binutils \
      devtoolset-6-gcc \
      devtoolset-6-gcc-c++ \
      git \
      openssl-devel \
      unzip \
      wget \
      which \
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
