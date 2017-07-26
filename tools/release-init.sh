#!/bin/bash
#
# Usage example: release-init.sh 2.6.8
#
# Description of release procedure can be found at https://github.com/realm/realm-wiki/wiki/Releasing-Realm-Core
#

realm_version=$(echo "$1" | sed -r -n '/^[0-9].[0-9].[0-9](-.*)?$/p')
if [ -z ${realm_version} ]; then
    echo Wrong version format: "$1"
    exit 1
fi

project_dir=$(git rev-parse --show-toplevel)

git branch release/${realm_version}
git push -u origin release/${realm_version}
git checkout -b prepare-$realm_version

# update dependencies.list
sed -i "s/^VERSION.*/VERSION=${realm_version}/" ${project_dir}/dependencies.list

RELEASE_HEADER="# $realm_version Release notes" || exit 1
sed -i "1s/.*/$RELEASE_HEADER/" ${project_dir}/CHANGELOG.md || exit 1

echo Ready to make ${realm_version}
