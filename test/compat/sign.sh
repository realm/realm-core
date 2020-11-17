#!/bin/sh
# A helper script to sign the token.
base64 < token.json > token.base64
openssl dgst -sha256 -binary -sign realm-private.pem < token.json | base64 > token.signed.base64
