#!/bin/bash
#
# Usage example: release-tag.sh "Feature release"
#
# Description of release procedure can be found at https://github.com/realm/realm-wiki/wiki/Releasing-Realm-Core
#

if [ $# != 1 ]; then
    echo Provide a description
    exit 1
fi

project_dir=$(git rev-parse --show-toplevel)
realm_version=$(grep VERSION "${project_dir}/dependencies.list" | cut -f 2 -d=)
tag=v${realm_version}
git tag -m \""$1"\" "${tag}"
git push origin "${tag}"
