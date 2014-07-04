#!/bin/bash
#
# Simple tool to create debian/ubuntu/mint packages from a source
# distribution (tar.gz) file.
#
# Usage ./create_debian_packages.sh 'RELEASE' 'destination'
#  'RELEASE'     = the name of the tar.gz file (absolute path)
#  'destination' = directory to store .deb files (absolute path)
#
# In order to use this tool, you must install all required packages.
# Please see the README.md files for all repositories for details.
# Moreover, you are required to install a few packages to assist you
# in creatung the deb files:
#
#     sudo apt-get -y install ntp debhelper fakeroot maven-debian-helper
#
# You run this script for each distribution/version/architecture,
# you wish to support.
#
# In order to create the apt repository used by customers, you must
# use a debian/ubuntu/mint based computer. Copy all the generated deb
# files to this central computer in one single folder (like
# $HOME/debs).
#
# A few helpers must be installed:
#
#     apt-get install reprepro dpkg-sig libterm-readkey-perl
#
# Create another folder (like $HOME/u) and within this folder, create
# a file named conf/distributions. The contents of this file consists
# of sections like:
#
# Origin: www.tightdb.com
# Label: tightdb ubuntu repository
# Codename: lucid
# Components: main
# Architectures: i386 amd64
# Description: TightDB for Ubuntu
# SignWith: 6C518649
#
# Each ubuntu (and mint) version have a codename. For each supported
# version, you create a section like the one above. The SignWith is
# the OpenGPG key that you sign the packages with (the dpkg-sig
# command below). You should also modify the Architectures line so it
# fits the supported cpu architectures.
#
# In the $HOME/debs folder (or whatever you called it), you must run
# the following commands:
#
#     dpkg-sig -k 6C518649 -p --sign builder *deb
#     reprepro --ask-passphrase -b $HOME/u includedeb lucid *lucid*deb
#
# The last command must be repeated for each supported version of
# ubuntu. If you decided to use something different than $HOME/u to
# create the apt repository, you should modify the second command
# accordingly.
#
# Finally, you can move the $HOME/u folder to the web server. As long
# as you are using WordPress (or any CMS), you must have a .htaccess
# file in order to get the apt repository to work:
#
# <IfModule mod_rewrite.c>
# RewriteEngine On
# RewriteRule (.*) $1
# </IfModule>
#
# Moreover, in order to help customers for easy installation you
# should create a small script at the web server named apt-tightdb.sh
# with the contents (add your OpenGPP key!!!):
#
# #!/bin/bash
# PATH=/bin:/usr/bin
#
# codename=$(lsb_release -s -c)
# case "$codename" in
#     "olivia")
#         codename="raring"
#         ;;
#     "maya")
#         codename="precise"
#         ;;
# esac
# echo "deb http://www.tightdb.com/u/ $codename main" > /etc/apt/sources.list.d/tightdb.list
#
# apt-key add - <<EOF
#   YOUR OpenGPG KEY
# EOF
#
# apt-get update
#

# save current directory
curdir=$(pwd)

release="$1"
if [ -z "$release" ]; then
    echo "No tar.gz release file"
    exit 1
fi

dest="$2"
if [ -z "$dest" ]; then
    echo "No destination directory"
    exit 1
fi

# setup
mkdir -p "$dest"

# virtual machine might have screwed clock
/etc/init.d/ntp status > /dev/null
if [ $? -ne 0 ]; then
    sudo ntpdate ntp.ubuntu.com
fi

# unpack tar.gz in tmp. directory
tmpdir=$(mktemp -d /tmp/$$.XXXXXX) || exit 1
cd $tmpdir
tar xzf $release || exit 1
cd $(find . -mindepth 1 -maxdepth 1 -type d)

# build core
cd tightdb
DISABLE_CHEETAH_CODE_GEN=1 sh build.sh dist-deb
cd ..
sudo dpkg -i *deb

# build extensions
for i in python java php ruby node gui; do
    cd tightdb_$i
    sh build.sh dist-deb
    cd ..
done

# remove installed tightdb packages
sudo dpkg -P $(dpkg-query -l '*tightdb*' | grep ^ii | awk '{print $2}')

# save a copy
cp -a *deb "$dest"

# clean up
cd $curdir
rm -rf $tmpdir
