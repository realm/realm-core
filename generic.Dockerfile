FROM centos:7

ARG CMAKE_VERSION=3.21.3

# Add the Oracle Linux Software Collections repository
RUN echo $' \n\
[ol7_software_collections] \n\
name=Software Collection packages for Oracle Linux 7 (\$basearch) \n\
baseurl=http://yum.oracle.com/repo/OracleLinux/OL7/SoftwareCollections/\$basearch/ \n\
gpgkey=https://yum.oracle.com/RPM-GPG-KEY-oracle-ol7 \n\
gpgcheck=1 \n\
enabled=1 \n\
' > /etc/yum.repos.d/OracleLinux-Software-Collections.repo

# Add the EPEL repository
RUN yum -y install \
      epel-release

RUN yum -y install \
      chrpath \
      devtoolset-10-binutils \
      devtoolset-10-gcc \
      devtoolset-10-gcc-c++ \
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
