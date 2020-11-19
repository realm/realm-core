#!/bin/bash
# This script fetches the specified package from S3

set -e

S3CFG=${S3CFG:-"$HOME/.s3cfg"}

filename=$1
url="s3://static.realm.io/downloads/sync-compat/$filename"

s3cmd --config=$S3CFG get "$url" "$filename"
