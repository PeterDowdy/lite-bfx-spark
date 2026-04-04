#!/usr/bin/env bash
# Shared script: build samtools from source and install to /usr/local.
# Expects SAMTOOLS_VERSION env var to be set (e.g. 1.21).
# Build deps must already be installed before calling this script.
set -euo pipefail

: "${SAMTOOLS_VERSION:?SAMTOOLS_VERSION must be set}"

curl -fsSL \
  "https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2" \
  | tar -xjf -

cd "samtools-${SAMTOOLS_VERSION}"
./configure
make -j"$(nproc)"
make install
cd ..
rm -rf "samtools-${SAMTOOLS_VERSION}"
