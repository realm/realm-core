FROM centos:7

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
      devtoolset-8-binutils \
      devtoolset-8-gcc \
      devtoolset-8-gcc-c++ \
      git \
      unzip \
      wget \
      which \
 && yum clean all
 
# Install CMake
RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"

# Install ninja
RUN git clone https://github.com/ninja-build/ninja.git \
    && cd ninja \
    && scl enable devtoolset-8 -- cmake -B build-cmake \
    && cmake --build build-cmake \
    && mv build-cmake/ninja /usr/bin
