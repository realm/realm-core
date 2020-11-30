apt-get update || exit 1
apt-get install -y "apt-transport-https" "ca-certificates" "curl" "gnupg" "gnupg-agent" "software-properties-common" || exit 1
add-apt-repository -y "ppa:ubuntu-toolchain-r/test" || exit 1
curl -fsSL "https://apt.kitware.com/keys/kitware-archive-latest.asc" | apt-key add - || exit 1
apt-add-repository -y "deb https://apt.kitware.com/ubuntu/ bionic main" || exit 1
apt-get update || exit 1
apt-get install -y "build-essential" "cmake" "ninja-build" || exit 1
apt-get install -y "gcc-9" "g++-9" || exit 1
ln -sf "gcc-9" "/usr/bin/gcc" || exit 1
ln -sf "g++-9" "/usr/bin/g++" || exit 1
