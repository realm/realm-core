FROM centos:7

# Install EPEL & devtoolset
RUN yum -y install \
      epel-release \
      centos-release-scl-rh

RUN yum -y install \
      chrpath \
      devtoolset-8-binutils \
      devtoolset-8-gcc \
      devtoolset-8-gcc-c++ \
      git \
      ninja-build \
      unzip \
      wget \
      which \
 && yum clean all
 
# Install CMake
RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"
