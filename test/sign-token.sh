#!/bin/bash

base64 -b 76 "$1" | while read line; do
    echo "    \"$line\""
done

echo "    \":\""

openssl dgst -sha256 -binary -sign test.pem "$1" | base64 -b 76 | while read line; do
    echo "    \"$line\""
done
