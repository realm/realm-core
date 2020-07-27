#!/bin/bash

set -e

function usage {
    echo "Usage: ${SCRIPT} <output_file> <input_file_a> <input_file_b>"
    exit 1;
}

function mergeLibs {
    local outputFile="$1"
    local fileA="$2"
    local fileB="$3"

    # Take architectures list
    IFS=' ' read -r -a archsA <<< `lipo -archs "$fileA"`
    IFS=' ' read -r -a archsB <<< `lipo -archs "$fileB"`

    # get all nonunique architectures
    test=($(comm -12 <(printf '%s\n' "${archsA[@]}" | LC_ALL=C sort) <(printf '%s\n' "${archsB[@]}" | LC_ALL=C sort)))

    if [ ${#test[@]} -eq 0 ]; then
        # if all archs are unique just lipo them
        lipo -create -output "$outputFile" "$fileA" "$fileB"
    elif [ ${#test[@]} -eq  ${#archsB[@]} ]; then
        # no archs to merge - copy input fileA to output
        cp "$fileA" "$outputFile"
    else
        # from the copy of the second lib
        tmpFile="${fileB}.tmp"

        # remove all duplicated archs
        printf -v removes -- '-remove %s ' "${test[@]}"
        lipo "$fileB" -output "$tmpFile" $removes

        # create a new library
        lipo -create -output "$outputFile" "$fileA" "$tmpFile"

        # delete a modified copy
        rm -f $tmpFile
    fi
}

outputFile="$1"
fileA="$2"
fileB="$3"

if [[ ! -e $fileA || ! -e $fileB ]]; then
    usage
fi

mergeLibs "$outputFile" "$fileA" "$fileB"
