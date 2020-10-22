#!/bin/bash

if [ -z $CMAKE_VARS_DIR ]; then
    echo "CMake variables directory must be set in CMAKE_VARS_DIR"
    exit 1
fi
if [ ! -d $CMAKE_VARS_DIR ]; then
    mkdir $CMAKE_VARS_DIR
fi


set_cmake_var() {
    file=$1
    var_name=$2
    var_type=$3
    shift; shift; shift;
    var_arg=$@
    if [ "$var_type" = "PATH" -o "$var_type" = "STRING" ]; then
        echo "set($var_name \"$var_arg\" CACHE $var_type \"\")" >> $CMAKE_VARS_DIR/$file.txt
    else
        echo "set($var_name $var_arg CACHE $var_type \"\")" >> $CMAKE_VARS_DIR/$file.txt
    fi
}
ensure_cmake_var_file() {
    touch $CMAKE_VARS_DIR/$1.txt
}
