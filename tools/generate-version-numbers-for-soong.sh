#!/bin/bash

source $1

version_and_extra=( ${VERSION//-/ } )
version_only=${version_and_extra[0]}
extra=${version_and_extra[1]}

semver=( ${version_only//./ } )
major=${semver[0]}
minor=${semver[1]}
patch=${semver[2]}

sed "s/@CONFIG_VERSION_MAJOR@/$major/g; s/@CONFIG_VERSION_MINOR@/$minor/g; s/@CONFIG_VERSION_PATCH@/$patch/g; s/@CONFIG_VERSION_TWEAK@/$extra/g; s/@CONFIG_VERSION@/$VERSION/g" $2
