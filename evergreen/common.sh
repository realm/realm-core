#!/bin/bash

set_cmake_var() {
    file=$1
    var_name=$2
    var_type=$3
    shift; shift; shift;
    var_arg=$@
    if [ "$var_type" = "PATH" -o "$var_type" = "STRING" ]; then
        echo "set($var_name \"$var_arg\" CACHE $var_type \"\")" >> cmake_vars/$file.txt
    else
        echo "set($var_name $var_arg CACHE $var_type \"\")" >> cmake_vars/$file.txt
    fi
}
ensure_cmake_var_file() {
    touch cmake_vars/$1.txt
}

if [ "$(uname -s)" = "Darwin" ]; then
    realpath() {
      OURPWD=$PWD
      cd "$(dirname "$1")"
      LINK=$(readlink "$(basename "$1")")
      while [ "$LINK" ]; do
        cd "$(dirname "$LINK")"
        LINK=$(readlink "$(basename "$1")")
      done
      REALPATH="$PWD/$(basename "$1")"
      cd "$OURPWD"
      echo "$REALPATH"
    }
fi
