#!/usr/bin/env bash
#
# Downloads the required certificate for the provided cluster, with a few
# assumptions:
# 1. bash, ccloud, and jq[1] are installed and on the user's $PATH
# 2. Users are already logged in via `ccloud auth login`
# 3. The provided cluster exists
#
# [1] https://jqlang.github.io/jq/

set -euo pipefail

if [[ "$UID" == "0" ]]; then
  echo "WARNING: Don't run this command as root outside of the Virtual Developer Sandbox (Instruqt)" >&1
fi

name=${1:-""}
if [[ -z "$name" || "$name" == "help" || "$name" == "--help" || "$name" == "-h" ]]; then
  cat >&1 <<EOF
Usage: $0 cluster_name

Example: $0 dijon-clustered
EOF
  exit 1
fi

cluster_id=$(ccloud cluster info --quiet --output json "$name" | jq --raw-output '.id')
curl \
  --silent \
  --create-dirs \
  --output "$HOME/.postgresql/root.crt" \
  "https://cockroachlabs.cloud/clusters/$cluster_id/cert"