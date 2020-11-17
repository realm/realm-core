#!/bin/bash
# This script uploads the specified versions (packaged by build.sh) to S3.
set -e

for sync_version in $@; do
    name="realm-build-$sync_version.$(uname).$(uname -m)"
    filename="$name.tar.gz"

    if [ ! -f "$filename" ]; then
        echo "$filename not found"
        exit 1
    fi

    url="s3://static.realm.io/downloads/sync-compat/$filename"
    s3cmd --continue-put put "$filename" "$url"
done
