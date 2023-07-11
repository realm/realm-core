#!/usr/bin/env bash

# This script uses 'minica' to simply create the certs
# On MacOS, install 'minica' using 'brew install minica'

## Server Alternative Names
CERT_DOMAINS="localhost"
CERT_IPADDRS="127.0.0.1,::1"

## Outputs
#  The key and certificate are placed in a new directory
#  whose name is chosen as the first domain name from the
#  certificate, or the first IP address if no domain names
#   are present.
ENTITY_DIR="localhost" # must match the first entry in CERT_DOMAINS
ENTITY_CERT_PEM="${ENTITY_DIR}/cert.pem"
ENTITY_KEY_PEM="${ENTITY_DIR}/key.pem"
ROOT_CERT_PEM="root-cert.pem"
ROOT_KEY_PEM="root-key.pem"

rm -f "${ENTITY_DIR}"
echo "Using root certificate ('${ROOT_CERT_PEM}') and key ('${ROOT_KEY_PEM}')"
minica -ca-cert "${ROOT_CERT_PEM}" -ca-key "${ROOT_KEY_PEM}" -domains "${CERT_DOMAINS}" -ip-addresses "${CERT_IPADDRS}"
echo "Generated new entity certificate ('${ENTITY_CERT_PEM}') and key ('${ENTITY_KEY_PEM}')"

