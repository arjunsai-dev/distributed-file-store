#!/usr/bin/env bash
# corrupt_chunk.sh <worker> <chunk_id>
# Flips one byte at offset 0 of the chunk file using docker compose exec + dd.
# Usage (from project root): ./scripts/corrupt_chunk.sh worker1 <chunk_id>

set -euo pipefail

WORKER="${1:?Usage: $0 <worker> <chunk_id>}"
CHUNK_ID="${2:?Usage: $0 <worker> <chunk_id>}"
CHUNK_PATH="/data/chunks/${CHUNK_ID}"

echo "[corrupt_chunk] worker=${WORKER} chunk=${CHUNK_ID} path=${CHUNK_PATH}"

# XOR first byte with 0xFF using dd + printf + dd conv=notrunc
docker compose exec -T "${WORKER}" sh -c "
  b=\$(dd if='${CHUNK_PATH}' bs=1 count=1 2>/dev/null | od -An -tu1 | tr -d ' ')
  new_b=\$(( b ^ 255 ))
  octal=\$(printf '%03o' \$new_b)
  printf \"\\\\\$octal\" | dd of='${CHUNK_PATH}' bs=1 count=1 conv=notrunc 2>/dev/null
"

echo "[corrupt_chunk] done — first byte of ${CHUNK_PATH} on ${WORKER} has been flipped"
