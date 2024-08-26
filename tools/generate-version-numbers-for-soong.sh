#!/bin/bash

version=$(awk '/^VERSION:[[:space:]]*/ {print $2}' $1)

major=$(echo $version | cut -d '.' -f 1)
minor=$(echo $version | cut -d '.' -f 2)
patch=$(echo $version | cut -d '.' -f 3)

patch_and_suffix=$(echo $version | cut -d '.' -f 3)

patch=${patch_and_suffix%%-*}
extra=${patch_and_suffix#*-}

if [[ "$extra" == "$patch_and_suffix" ]]; then
    extra=""
fi

sed "s/@CONFIG_VERSION_MAJOR@/$major/g; s/@CONFIG_VERSION_MINOR@/$minor/g; s/@CONFIG_VERSION_PATCH@/$patch/g; s/@CONFIG_VERSION_TWEAK@/$extra/g; s/@CONFIG_VERSION@/$version/g" $2
