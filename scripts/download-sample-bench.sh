#!/bin/bash

set +ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [[ -z "${CI}" ]] && [[ -f "${SCRIPT_DIR}/../tests/resources/taxonomy-nodes.nq" ]]; then
    echo "dependencies present"
    exit 0
fi

sudo curl -L  https://download.bio2rdf.org/files/release/4/taxonomy/taxonomy-nodes.nq.gz -o $SCRIPT_DIR/../tests/resources/taxonomy-nodes.nq.gz
sudo apt-get install gzip -y
gzip -d $SCRIPT_DIR/../tests/resources/taxonomy-nodes.nq.gz
