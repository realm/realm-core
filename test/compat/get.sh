#!/bin/bash
# This script fetches the specified versions of packages from S3 which have
# been built by build.sh and put there by put.sh.

set -e

S3CFG=${S3CFG:-"$HOME/.s3cfg"}

for sync_version in $@; do
    name="realm-build-$sync_version"
    filename="$name.tar.gz"

    if [ -f "$filename" ]; then
        echo "$filename has already been received"
        exit 0
    fi

    url="s3://static.realm.io/downloads/sync-compat/$filename"
    s3cmd --config=$S3CFG get "$url" "$filename"
done
