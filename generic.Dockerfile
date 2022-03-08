FROM centos:7

ARG CMAKE_VERSION=3.22.2

# Add the EPEL and CentOS SCLo RH repositories
RUN yum -y install \
      centos-release-scl-rh \
      epel-release

RUN yum -y install \
      chrpath \
      devtoolset-11-binutils \
      devtoolset-11-gcc \
      devtoolset-11-gcc-c++ \
      git \
      ninja-build \
      unzip \
      which \
      zlib-devel \
 && yum clean all
 
# Install CMake
RUN cd /opt \
    && curl -LO https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && tar zxvf cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && rm -f cmake-$CMAKE_VERSION-linux-x86_64.tar.gz
ENV PATH "/opt/cmake-$CMAKE_VERSION-linux-x86_64/bin:$PATH"

ENV GIT_COMMITTER_NAME realm-ci
ENV GIT_COMMITTER_EMAIL ci@realm.io
