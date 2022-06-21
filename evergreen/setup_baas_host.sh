#!/bin/bash

set -o errexit
set -o pipefail

BAAS_BRANCH=
trap 'catch $? $LINENO' EXIT
catch() {
  echo "remote install_baas.sh wrapper exiting"
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting baas on $2"
  fi
}

usage()
{
    echo "Usage: setup_baas_host.sh "
    echo "         [-b branch of baas to checkout]"
    echo "         [-a path to aws credentials script]"
    echo "         [-x]"
    exit 0
}

PASSED_ARGUMENTS=$(getopt b:a:x "$*") || usage
set -- $PASSED_ARGUMENTS
while :
do
  case "$1" in
    -b) BAAS_BRANCH="$2"; shift; shift;;
    -a) AWS_CREDS_SCRIPT="$2"; shift; shift;;
    -x) SETUP_FILESYSTEMS=yes; shift;;
    --) shift; break;;
    *) echo "Unexpected option $1"; usage;;
  esac
done

if [[ -z $AWS_CREDS_SCRIPT ]]; then
    echo "Must spply path to secrets script"
    exit 1
fi

source $AWS_CREDS_SCRIPT
if [[ -n "$SETUP_FILESYSTEMS" ]]; then
    sudo umount /mnt 2>/dev/null || true
    sudo umount /dev/xvdb 2>/dev/null|| true
    sudo /sbin/mkfs.xfs -f /dev/xvdb
    sudo mkdir -p /data
    echo "/dev/xvdb /data auto noatime 0 0" | sudo tee -a /etc/fstab
    sudo mount /data
    sudo chown -R ubuntu:ubuntu /data

    mkdir /data/tmp
    chmod 1777 /data/tmp

    echo "github.com,207.97.227.239 ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" | tee -a /home/ubuntu/.ssh/known_hosts
    sudo chmod 600 /home/ubuntu/.ssh/*
fi

cd /data

git clone git@github.com:realm/realm-core.git realm-core
cd realm-core
git checkout $REALM_CORE_REVISION

git submodule update --init --recursive
mv /home/ubuntu/install_baas.sh evergreen/

install_baas_flags="-w ./baas-work-dir"
if [[ -n "$BAAS_BRANCH" ]]; then
    install_baas_flags="$install_baas_flags -b $BAAS_BRANCH"
fi

./evergreen/install_baas.sh $install_baas_flags 2>&1 
