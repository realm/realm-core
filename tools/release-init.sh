#!/bin/bash
#
# Usage example: release-init.sh 2.6.8
#
# Description of release procedure can be found at https://github.com/realm/realm-wiki/wiki/Releasing-Realm-Core
#

VERSION_GREP='^[0-9]?[0-9].[0-9]+.[0-9]+(-.*)?$'

function usage()
{
    echo "Usage: release-init.sh VERSION"
    echo "  VERSION format regex: ${VERSION_GREP}"
    echo "  Examples:"
    echo "    10.123.1"
    echo "    99.999999.999999"
    echo "    4.5.0-CustDemo"
    exit 1
}

if [[ -z "$1" ]]; then
    usage
fi

# egrep has been replaced with 'grep -E'
realm_version=$(echo "$1" | grep -E "${VERSION_GREP}")
if [ -z "${realm_version}" ]; then
    echo Wrong version format: "$1"
    usage
fi

# make sure submodules are up to date
git submodule update --init --recursive

project_dir=$(git rev-parse --show-toplevel)

git branch release/${realm_version}
git push -u origin release/${realm_version}
git checkout -b prepare-$realm_version

# update dependencies.yml
sed -i.bak -e "s/^VERSION.*/VERSION: ${realm_version}/" "${project_dir}/dependencies.yml"
rm "${project_dir}/dependencies.yml.bak" || exit 1

# update Package.swift
sed -i.bak -e "s/^let versionStr =.*/let versionStr = \"${realm_version}\"/" "${project_dir}/Package.swift"
rm "${project_dir}/Package.swift.bak" || exit 1

RELEASE_HEADER="# $realm_version Release notes" || exit 1
sed -i.bak -e "1s/.*/$RELEASE_HEADER/" "${project_dir}/CHANGELOG.md" || exit 1
rm "${project_dir}/CHANGELOG.md.bak" || exit 1

echo Ready to make "${realm_version}"
