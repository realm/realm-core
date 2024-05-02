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
realm_version=$(sed -rn 's/^VERSION: (.*)/\1/p' < "${project_dir}/dependencies.yml")
tag=v${realm_version}
git tag -m \""$1"\" "${tag}"
git push origin "${tag}"
