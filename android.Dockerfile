FROM ubuntu:16.04

# Locales
RUN apt-get clean && apt-get update -qq && apt-get install -y locales && locale-gen en_US.UTF-8
ENV LANG "en_US.UTF-8"
ENV LANGUAGE "en_US.UTF-8"
ENV LC_ALL "en_US.UTF-8"

# Keep the packages in alphabetical order to make it easy to avoid duplication
RUN DEBIAN_FRONTEND=noninteractive dpkg --add-architecture i386 \
    && apt-get update -qq \
    && apt-get install -y adb \
                          build-essential \
                          curl \
                          git \
                          libuv1-dev \
                          ninja-build \
                          ruby \
                          ruby-dev \
                          s3cmd \
                          unzip \
                          xutils-dev \
                          tar \
                          wget \
                          zip \
    && apt-get clean

# Install CMake
RUN cd /opt \
    && wget -nv https://cmake.org/files/v3.15/cmake-3.15.2-Linux-x86_64.tar.gz \
    && tar zxf cmake-3.15.2-Linux-x86_64.tar.gz

ENV PATH "/opt/cmake-3.15.2-Linux-x86_64/bin:$PATH"

# Install the NDK
RUN cd /opt \
    && wget -nv https://dl.google.com/android/repository/android-ndk-r21-linux-x86_64.zip \
    && unzip -q android-ndk-r21-linux-x86_64.zip \
    && rm -f android-ndk-r21-linux-x86_64.zip

ENV ANDROID_NDK_ROOT "/opt/android-ndk-r21"
ENV ANDROID_NDK_HOME "/opt/android-ndk-r21"
ENV ANDROID_NDK "/opt/android-ndk-r21"
