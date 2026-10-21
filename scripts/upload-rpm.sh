#!/usr/bin/env bash
# GITEA_REGISTRY_TOKEN uses the same username:token format as bec_log_ingestor.
set -euo pipefail

: "${GITEA_REGISTRY_TOKEN:?Set GITEA_REGISTRY_TOKEN to username:token}"
shopt -s nullglob
packages=("${1:-dist}"/*.rpm)
if ((${#packages[@]} == 0)); then
  echo 'No RPM packages found; refusing to publish an empty release.' >&2
  exit 1
fi

for package in "${packages[@]}"; do
  echo "Uploading $(basename "$package") to the BEC RPM registry"
  curl --fail-with-body --silent --show-error \
    --connect-timeout 15 --max-time 300 \
    --user "$GITEA_REGISTRY_TOKEN" \
    --upload-file "$package" \
    'https://gitea.psi.ch/api/packages/bec/rpm/upload'
done
