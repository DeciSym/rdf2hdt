#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEST_DIR="${SCRIPT_DIR}/../tests/resources"
DEST_NQ="${DEST_DIR}/taxonomy-nodes.nq"

if [[ -z "${CI:-}" ]] && [[ -f "${DEST_NQ}" ]]; then
    echo "dependencies present"
    exit 0
fi

mkdir -p "${DEST_DIR}"
curl --fail --location --show-error \
    "https://download.bio2rdf.org/files/release/4/taxonomy/taxonomy-nodes.nq.gz" \
    -o "${DEST_NQ}.gz"
gunzip -f "${DEST_NQ}.gz"
