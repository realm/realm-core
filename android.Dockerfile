FROM ubuntu:20.04

ARG NDK_VERSION=r22
ARG CMAKE_VERSION=3.20.0

ENV DEBIAN_FRONTEND "noninteractive" 

# Locales
RUN apt-get clean && apt-get update -qq && apt-get install -y locales && locale-gen en_US.UTF-8
ENV LANG "en_US.UTF-8"
ENV LANGUAGE "en_US.UTF-8"
ENV LC_ALL "en_US.UTF-8"

# Keep the packages in alphabetical order to make it easy to avoid duplication
RUN apt-get update -qq \
    && apt-get install -y adb \
                          build-essential \
                          git \
                          ninja-build \
                          unzip \
                          tar \
                          wget \
                          zip \
    && apt-get clean

# Install CMake
RUN cd /opt \
    && wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-linux-x86_64.tar.gz \
    && tar zxvf cmake-$CMAKE_VERSION-linux-x86_64.tar.gz

ENV PATH "/opt/cmake-$CMAKE_VERSION-linux-x86_64/bin:$PATH"

# Install the NDK
RUN cd /opt \
    && wget https://dl.google.com/android/repository/android-ndk-$NDK_VERSION-linux-x86_64.zip \
    && unzip android-ndk-$NDK_VERSION-linux-x86_64.zip \
    && rm -f android-ndk-$NDK_VERSION-linux-x86_64.zip
    
ENV ANDROID_NDK "/opt/android-ndk-$NDK_VERSION"
