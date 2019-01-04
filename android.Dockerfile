FROM ubuntu:16.04

# Locales
RUN apt-get clean && apt-get update -qq && apt-get install -y locales && locale-gen en_US.UTF-8
ENV LANG "en_US.UTF-8"
ENV LANGUAGE "en_US.UTF-8"
ENV LC_ALL "en_US.UTF-8"

# The 32 bit binaries because aapt requires it
# `file` is need by the script that creates NDK toolchains
# Keep the packages in alphabetical order to make it easy to avoid duplication
RUN DEBIAN_FRONTEND=noninteractive dpkg --add-architecture i386 \
    && apt-get update -qq \
    && apt-get install -y adb \
                          bsdmainutils \
                          build-essential \
                          curl \
                          file \
                          git \
                          libc6:i386 \
                          libgcc1:i386 \
                          libncurses5:i386 \
                          libstdc++6:i386 \
                          libz1:i386 \
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
    && wget https://cmake.org/files/v3.7/cmake-3.7.2-Linux-x86_64.tar.gz \
    && tar zxvf cmake-3.7.2-Linux-x86_64.tar.gz

ENV PATH "$PATH:/opt/cmake-3.7.2-Linux-x86_64/bin"

# Install the NDK
RUN cd /opt \
    && wget https://dl.google.com/android/repository/android-ndk-r18b-linux-x86_64.zip \
    && unzip android-ndk-r18b-linux-x86_64.zip \
    && rm -f android-ndk-r18b-linux-x86_64.zip

ENV ANDROID_NDK_ROOT "/opt/android-ndk-r18b"
ENV ANDROID_NDK_HOME "/opt/android-ndk-r18b"
ENV ANDROID_NDK "/opt/android-ndk-r18b"
