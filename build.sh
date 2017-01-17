#!/bin/sh

case "$1" in
    "config")
        rake config["$2"]
        ;;
    *)
        rake "$@"
        ;;
esac
