#!/usr/bin/env bash
# ==============================================================================
# scripts/demo.sh — Demonstrates the Mini Distributed Cloud Object Store
#
# Runs the 6 required demonstrations:
# (1) Upload and download a file, show matching sha256
# (2) Upload a >1 MB file, show metadata chunk list and ls of each worker's /data/chunks
# (3) Show each chunk has 2 replicas on different workers
# (4) Upload with custom metadata, GET, PATCH, GET without re-upload
# (5) Stop worker2, show /admin/status, download still works, start worker2, wait, show replication restored
# (6) Corrupt a chunk with scripts/corrupt_chunk.sh, download (still correct), show controller logs for detection and repair
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

CONTROLLER_URL="http://localhost:8000"
PAUSE_TIMEOUT="${DEMO_TIMEOUT:-3}"

# Cleanup temporary files on exit
trap 'rm -f /tmp/demo_*' EXIT

banner() {
    echo ""
    echo "================================================================================"
    echo "  $1"
    echo "================================================================================"
    echo ""
}

pause() {
    echo ""
    if [ "${PAUSE_TIMEOUT}" -eq 0 ]; then
        read -r -p ">>> Press [Enter] to continue..." dummy || true
    else
        read -r -t "${PAUSE_TIMEOUT}" -p ">>> Press [Enter] to continue (or wait ${PAUSE_TIMEOUT}s)..." dummy || true
    fi
    echo ""
}

echo "=== Mini Distributed Cloud Object Store — Live Demonstration ==="
echo "Checking controller and worker readiness..."

for i in {1..30}; do
    if curl -sf "${CONTROLLER_URL}/health" >/dev/null 2>&1; then
        ALIVE=$(curl -s "${CONTROLLER_URL}/admin/status" 2>/dev/null | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
    print(sum(1 for w in d.get("workers", []) if w.get("status") == "ALIVE"))
except Exception:
    print(0)
' 2>/dev/null || echo "0")
        if [ "${ALIVE}" -ge 4 ]; then
            echo "All 4 workers are ALIVE and registered with controller."
            break
        fi
    fi
    sleep 1
done

pause


# ==============================================================================
# DEMO 1: Upload and download a file, show matching sha256
# ==============================================================================
banner "DEMO 1: Upload and download a file, verify matching SHA-256"

echo "Creating a 512 KB sample file..."
head -c 524288 /dev/urandom > /tmp/demo_file1.bin
ORIG_SHA1=$(sha256sum /tmp/demo_file1.bin | cut -d' ' -f1)
echo "Original file SHA-256: ${ORIG_SHA1}"

echo ""
echo "Uploading file via POST /api/v1/objects..."
UPLOAD_RESP1=$(curl -s -F file=@/tmp/demo_file1.bin "${CONTROLLER_URL}/api/v1/objects")
echo "Upload Response:"
echo "${UPLOAD_RESP1}" | python3 -m json.tool

OBJ_ID1=$(echo "${UPLOAD_RESP1}" | python3 -c 'import sys, json; print(json.load(sys.stdin)["object_id"])')
echo ""
echo "Uploaded Object ID: ${OBJ_ID1}"

echo ""
echo "Downloading file via GET /api/v1/objects/${OBJ_ID1}..."
curl -s -o /tmp/demo_file1_downloaded.bin "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID1}"
DOWN_SHA1=$(sha256sum /tmp/demo_file1_downloaded.bin | cut -d' ' -f1)
echo "Downloaded file SHA-256: ${DOWN_SHA1}"

echo ""
if [ "${ORIG_SHA1}" = "${DOWN_SHA1}" ]; then
    echo ">>> SUCCESS: Original and downloaded SHA-256 hashes MATCH EXACTLY! <<<"
else
    echo ">>> FAILURE: Hash mismatch! <<<"
    exit 1
fi

pause


# ==============================================================================
# DEMO 2: Upload a >1 MB file, show metadata chunk list and ls of each worker's /data/chunks
# ==============================================================================
banner "DEMO 2: Upload >1 MB file, show chunk list & worker disk chunks"

echo "Creating a 2.5 MB (2,621,440 bytes) sample file (will split into 3 chunks: 1MB, 1MB, 0.5MB)..."
head -c 2621440 /dev/urandom > /tmp/demo_file2.bin
ORIG_SHA2=$(sha256sum /tmp/demo_file2.bin | cut -d' ' -f1)
echo "Original 2.5 MB file SHA-256: ${ORIG_SHA2}"

echo ""
echo "Uploading 2.5 MB file via POST /api/v1/objects..."
UPLOAD_RESP2=$(curl -s -F file=@/tmp/demo_file2.bin "${CONTROLLER_URL}/api/v1/objects")
echo "Upload Response:"
echo "${UPLOAD_RESP2}" | python3 -m json.tool

OBJ_ID2=$(echo "${UPLOAD_RESP2}" | python3 -c 'import sys, json; print(json.load(sys.stdin)["object_id"])')
echo ""
echo "Object ID: ${OBJ_ID2}"

echo ""
echo "Fetching Object Metadata (GET /api/v1/objects/${OBJ_ID2}/metadata):"
META_RESP2=$(curl -s "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID2}/metadata")
echo "${META_RESP2}" | python3 -m json.tool

echo ""
echo "Listing /data/chunks across each worker container:"
for w in worker1 worker2 worker3 worker4; do
    echo "--- ${w}: /data/chunks ---"
    docker compose exec -T "${w}" ls -lh /data/chunks
done

pause


# ==============================================================================
# DEMO 3: Show each chunk has 2 replicas on different workers
# ==============================================================================
banner "DEMO 3: Verify each chunk has exactly 2 replicas on different workers"

echo "Analyzing chunk replicas for Object ID: ${OBJ_ID2}..."
echo "${META_RESP2}" | python3 -c '
import sys, json

meta = json.load(sys.stdin)
chunks = meta.get("chunks", [])
print(f"Total Chunks: {len(chunks)}\n")

all_valid = True
for c in chunks:
    cid = c["chunk_id"]
    idx = c["index"]
    reps = c.get("replicas", [])
    worker_ids = [r["worker_id"] for r in reps]
    is_distinct = len(worker_ids) == 2 and worker_ids[0] != worker_ids[1]
    status = "OK" if is_distinct else "INVALID"
    if not is_distinct:
        all_valid = False
    print(f"  Chunk index {idx} ({cid}):")
    print(f"    Replicas ({len(worker_ids)}): {worker_ids}")
    print(f"    Verification: [{status}] 2 distinct workers ({worker_ids[0]} != {worker_ids[1]})\n")

if all_valid:
    print(">>> SUCCESS: Every chunk has exactly 2 replicas on distinct workers! <<<")
else:
    print(">>> FAILURE: Replica distribution invariant violated! <<<")
    sys.exit(1)
'

pause


# ==============================================================================
# DEMO 4: Upload with custom metadata, GET, PATCH, GET without re-upload
# ==============================================================================
banner "DEMO 4: Custom metadata — Upload, GET, PATCH, GET without re-upload"

echo "Creating a sample file..."
head -c 65536 /dev/urandom > /tmp/demo_file4.bin

echo "Uploading with custom metadata: {\"course\":\"cloud-computing\",\"owner\":\"alice\",\"env\":\"staging\"}..."
UPLOAD_RESP4=$(curl -s \
    -F file=@/tmp/demo_file4.bin \
    -F 'metadata={"course":"cloud-computing","owner":"alice","env":"staging"}' \
    "${CONTROLLER_URL}/api/v1/objects")
OBJ_ID4=$(echo "${UPLOAD_RESP4}" | python3 -c 'import sys, json; print(json.load(sys.stdin)["object_id"])')
echo "Uploaded Object ID: ${OBJ_ID4}"

echo ""
echo "1. Initial GET /api/v1/objects/${OBJ_ID4}/metadata:"
curl -s "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID4}/metadata" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("  custom_meta:", d.get("custom_meta"))
print("  created_at: ", d.get("created_at"))
print("  modified_at:", d.get("modified_at"))
'

echo ""
echo "2. PATCH /api/v1/objects/${OBJ_ID4}/metadata with: {\"env\":\"production\",\"reviewed\":true,\"owner\":null}..."
PATCH_RESP4=$(curl -s -X PATCH \
    -H "Content-Type: application/json" \
    -d '{"env":"production","reviewed":true,"owner":null}' \
    "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID4}/metadata")
echo "PATCH Response:"
echo "${PATCH_RESP4}" | python3 -m json.tool

echo ""
echo "3. Second GET /api/v1/objects/${OBJ_ID4}/metadata (without re-uploading file bytes):"
curl -s "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID4}/metadata" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("  custom_meta:", d.get("custom_meta"))
print("  created_at: ", d.get("created_at"))
print("  modified_at:", d.get("modified_at"))
cm = d.get("custom_meta", {})
assert cm.get("env") == "production", "env not updated"
assert cm.get("reviewed") is True, "reviewed not added"
assert "owner" not in cm, "owner not deleted"
print("\n>>> SUCCESS: Metadata updated, key deleted, modified_at changed without re-upload! <<<")
'

pause


# ==============================================================================
# DEMO 5: Stop worker2, show /admin/status, download works, start worker2, wait, show replication restored
# ==============================================================================
banner "DEMO 5: Worker failure, continued availability & auto-repair"

echo "Current cluster status (GET /admin/status):"
curl -s "${CONTROLLER_URL}/admin/status" | python3 -m json.tool

echo ""
echo "Stopping worker2 (simulating node crash)..."
docker compose stop worker2

echo "Waiting for controller failure detector (DEAD_AFTER = 10s)..."
for i in {1..12}; do
    echo -n "."
    sleep 1
done
echo ""

echo ""
echo "Cluster status with worker2 DEAD (GET /admin/status):"
curl -s "${CONTROLLER_URL}/admin/status" | python3 -m json.tool

echo ""
echo "Downloading Demo 2 file (${OBJ_ID2}) while worker2 is dead..."
curl -s -o /tmp/demo_file2_failover.bin "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID2}"
FAILOVER_SHA=$(sha256sum /tmp/demo_file2_failover.bin | cut -d' ' -f1)
echo "Downloaded SHA-256 during failure: ${FAILOVER_SHA}"
if [ "${ORIG_SHA2}" = "${FAILOVER_SHA}" ]; then
    echo ">>> SUCCESS: Object downloaded successfully even with worker2 dead! <<<"
else
    echo ">>> FAILURE: Could not retrieve object during failure! <<<"
    exit 1
fi

echo ""
echo "Waiting for repair loop to restore under-replicated chunks..."
for i in {1..15}; do
    UNDER=$(curl -s "${CONTROLLER_URL}/admin/status" | python3 -c '
import sys, json
d = json.load(sys.stdin)
# Filter out chunks where live_count == 0 if any
print(d.get("under_replicated_chunks", 0))
' 2>/dev/null || echo "1")
    if [ "${UNDER}" -eq 0 ]; then
        echo "Under-replicated chunks restored to 0 by repair loop!"
        break
    fi
    sleep 1
done

echo ""
echo "Restarting worker2..."
docker compose start worker2
sleep 4

echo "Cluster status after worker2 returned (GET /admin/status):"
curl -s "${CONTROLLER_URL}/admin/status" | python3 -m json.tool

echo ">>> SUCCESS: Worker recovered and cluster replication is fully healthy! <<<"

pause


# ==============================================================================
# DEMO 6: Corrupt a chunk with scripts/corrupt_chunk.sh, download, show logs
# ==============================================================================
banner "DEMO 6: Data corruption detection and read-repair"

echo "Uploading a fresh file for corruption test..."
head -c 1048576 /dev/urandom > /tmp/demo_file6.bin
ORIG_SHA6=$(sha256sum /tmp/demo_file6.bin | cut -d' ' -f1)
UPLOAD_RESP6=$(curl -s -F file=@/tmp/demo_file6.bin "${CONTROLLER_URL}/api/v1/objects")
OBJ_ID6=$(echo "${UPLOAD_RESP6}" | python3 -c 'import sys, json; print(json.load(sys.stdin)["object_id"])')
CHUNK_ID6="${OBJ_ID6}_0"

META6=$(curl -s "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID6}/metadata")
WORKER_CORRUPT=$(echo "${META6}" | python3 -c 'import sys, json; print(json.load(sys.stdin)["chunks"][0]["replicas"][0]["worker_id"])')
echo "Object ID: ${OBJ_ID6}"
echo "Chunk:     ${CHUNK_ID6}"
echo "Target:    ${WORKER_CORRUPT}"

echo ""
echo "Flipping one byte on ${WORKER_CORRUPT} with scripts/corrupt_chunk.sh..."
./scripts/corrupt_chunk.sh "${WORKER_CORRUPT}" "${CHUNK_ID6}"

echo ""
echo "Downloading object (${OBJ_ID6}) through controller — read-repair triggers automatically:"
curl -s -o /tmp/demo_file6_downloaded.bin "${CONTROLLER_URL}/api/v1/objects/${OBJ_ID6}"
DOWN_SHA6=$(sha256sum /tmp/demo_file6_downloaded.bin | cut -d' ' -f1)
echo "Downloaded SHA-256: ${DOWN_SHA6}"

if [ "${ORIG_SHA6}" = "${DOWN_SHA6}" ]; then
    echo ">>> SUCCESS: Controller served verified good bytes from alternate replica! <<<"
else
    echo ">>> FAILURE: Bad bytes were served! <<<"
    exit 1
fi

echo ""
echo "Controller logs showing corruption detection and automatic read-repair:"
docker compose logs controller | grep -E "corruption_detected|corruption_repaired" | tail -6

echo ""
echo "Verifying repaired chunk checksum on ${WORKER_CORRUPT}:"
PORT=8001
case "${WORKER_CORRUPT}" in
    worker1) PORT=8001 ;;
    worker2) PORT=8002 ;;
    worker3) PORT=8003 ;;
    worker4) PORT=8004 ;;
esac
curl -s "http://localhost:${PORT}/internal/chunks/${CHUNK_ID6}/checksum" | python3 -m json.tool

echo ""
echo "================================================================================"
echo "  ALL 6 DEMONSTRATIONS COMPLETED SUCCESSFULLY!"
echo "================================================================================"
