#!/bin/bash
#
# Usage example: release-init.sh 2.6.8
#
# Description of release procedure can be found at https://github.com/realm/realm-core/doc/development/how-to-release.md
#

set -e -x

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

# update dependencies.yml
sed -i.bak -e "s/^VERSION.*/VERSION: ${realm_version}/" "${project_dir}/dependencies.yml"
rm "${project_dir}/dependencies.yml.bak" || exit 1

# update Package.swift
sed -i.bak -e "s/^let versionStr =.*/let versionStr = \"${realm_version}\"/" "${project_dir}/Package.swift"
rm "${project_dir}/Package.swift.bak" || exit 1

# update CHANGELOG.md
RELEASE_HEADER="# $realm_version Release notes" || exit 1
sed -i.bak -e "1s/.*/$RELEASE_HEADER/" "${project_dir}/CHANGELOG.md" || exit 1
sed -i.bak -e "/.*\[#????\](https.*/d" "${project_dir}/CHANGELOG.md"
rm "${project_dir}/CHANGELOG.md.bak" || exit 1

# make the PR description
PR_BODY_FILE="pr-body.txt"
# assumes that tags and history have been fetched
GIT_HASH=$(git rev-parse HEAD)
# if you wish to run this locally you need to install the github cli (https://cli.github.com/manual)
# both the gh and jq are available in github actions natively
GH_STATUS_FOR_COMMIT=$(gh api -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" /repos/realm/realm-core/commits/$GIT_HASH/status)
GH_CHECK_STATE=$(echo $GH_STATUS_FOR_COMMIT | jq '.state')
echo "CI checks for $GIT_HASH:" > $PR_BODY_FILE
if [ "$GH_CHECK_STATE" = '"success"' ]; then
  echo "    succeeded! :white_check_mark:" >> $PR_BODY_FILE
else
  echo "    failed! :x:" >> $PR_BODY_FILE
fi
echo "You may also want to manually verify the [evergreen checks](https://spruce.mongodb.com/commits/realm-core-stable)" >> $PR_BODY_FILE
echo "" >> $PR_BODY_FILE
echo "commits since last tag:" >> $PR_BODY_FILE
git log $(git describe --tags --abbrev=0)..HEAD --oneline --no-merges >> $PR_BODY_FILE
echo "The PR body file contains:"
cat $PR_BODY_FILE

echo Ready to make "${realm_version}"

