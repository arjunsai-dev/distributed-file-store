# Mini Distributed Cloud Object Store

A fault-tolerant, chunked distributed cloud object store inspired by Google File System (GFS) and Amazon S3, implemented in Python 3.12 with FastAPI, Docker Compose, and SQLite.

---

## 1. Architecture Overview

The system consists of **1 Controller** and **4 Workers** running inside a private Docker Compose network:

- **Controller (`http://localhost:8000`)**: Manages metadata, handles client upload/download/metadata requests, determines chunk placement, monitors worker heartbeats, triggers automated repair, and runs a periodic scrubber.
- **Workers (`worker1`–`worker4`, published on `8001`–`8004`)**: Stateless HTTP storage nodes. Each worker persists chunks to its own dedicated Docker named volume mounted at `/data/chunks`.
- **Metadata Database**: SQLite in WAL mode on the controller (`/data/controller.db`) protected by a global thread lock.

```
                             +-----------------------+
                             |     Client / User     |
                             +-----------------------+
                                         |
                                         | Public REST API (/api/v1)
                                         v
                             +-----------------------+
                             |      CONTROLLER       |
                             |  (:8000 in Docker)    |
                             |                       |
                             |  * Placement Engine   |
                             |  * Failure Detector   |
                             |  * Repair Loop        |
                             |  * Scrubber Loop      |
                             |  * SQLite DB (WAL)    |
                             +-----------------------+
                                  /    |    \    \
        +------------------------+     |     +----+-------------------+
        | Internal HTTP PUT/GET/DELETE | (Replication & Heartbeats)   |
        v                              v                              v
+---------------+              +---------------+              +---------------+
|    WORKER 1   |              |    WORKER 2   |              |  WORKER 3 & 4 |
| (:8001->8000) |              | (:8002->8000) |              | (:8003,:8004) |
| Volume: /data |              | Volume: /data |              | Volume: /data |
+---------------+              +---------------+              +---------------+
```

---

## 2. Prerequisites

- **Docker & Docker Compose** (Docker Desktop on macOS/Windows or Docker Engine 24+ on Linux)
- **Bash & cURL** (for interacting with endpoints and running demo scripts)
- **Python 3.12+** (optional, for parsing JSON responses with `python3 -m json.tool`)

---

## 3. Build & Start Commands

### Start Cluster
```bash
docker compose up --build -d
```

### Check Cluster Status
```bash
docker compose ps
curl -s http://localhost:8000/health
curl -s http://localhost:8000/admin/status | python3 -m json.tool
```

### View Live Logs
```bash
# Controller logs (monitoring, placement, repairs, scrub events)
docker compose logs -f controller

# Specific worker logs
docker compose logs -f worker1
```

### Stop & Reset Cluster (Wipe Data)
```bash
docker compose down -v
```

---

## 4. API Reference & Curl Examples

### Public API (`/api/v1`)

#### 1. Upload Object — `POST /api/v1/objects`
Uploads a file using multipart form-data with an optional JSON metadata string. Files are streamed in 1 MB chunks and replicated to 2 distinct alive workers.
```bash
curl -F "file=@sample.pdf" \
     -F 'metadata={"author":"alice","project":"mini-store"}' \
     http://localhost:8000/api/v1/objects
```
*Response (201 Created):*
```json
{
  "object_id": "0f62b216-9b57-4bf7-a9a7-ff70f7d45c58",
  "name": "sample.pdf",
  "size": 2500000,
  "content_type": "application/pdf",
  "created_at": "2026-11-10T17:00:00Z",
  "modified_at": "2026-11-10T17:00:00Z",
  "checksum": "d2ea7c5e70e1...",
  "num_chunks": 3,
  "custom_meta": {"author": "alice", "project": "mini-store"},
  "status": "COMPLETE"
}
```

#### 2. Download Object — `GET /api/v1/objects/{object_id}`
Streams the binary file in chunk order. Automatically performs checksum validation and transparent read-repair if a replica is corrupt.
```bash
curl -o downloaded.pdf http://localhost:8000/api/v1/objects/0f62b216-9b57-4bf7-a9a7-ff70f7d45c58
```

#### 3. Get Object Metadata — `GET /api/v1/objects/{object_id}/metadata`
Returns system fields, custom metadata, and chunk layout with replica worker IDs and their alive statuses.
```bash
curl -s http://localhost:8000/api/v1/objects/0f62b216-9b57-4bf7-a9a7-ff70f7d45c58/metadata | python3 -m json.tool
```

#### 4. Update Custom Metadata — `PATCH /api/v1/objects/{object_id}/metadata`
Merges custom metadata without re-uploading file bytes. Setting a key to `null` deletes the key.
```bash
curl -X PATCH -H "Content-Type: application/json" \
     -d '{"project": "distributed-cc", "reviewed": true, "author": null}' \
     http://localhost:8000/api/v1/objects/0f62b216-9b57-4bf7-a9a7-ff70f7d45c58/metadata
```

#### 5. List Complete Objects — `GET /api/v1/objects`
Lists COMPLETE objects with pagination.
```bash
curl -s "http://localhost:8000/api/v1/objects?limit=20&offset=0" | python3 -m json.tool
```

#### 6. Delete Object — `DELETE /api/v1/objects/{object_id}`
Marks status to `DELETING`, removes database records, and issues DELETEs to replica workers.
```bash
curl -i -X DELETE http://localhost:8000/api/v1/objects/0f62b216-9b57-4bf7-a9a7-ff70f7d45c58
```
*Response: `204 No Content`*

---

### Admin & Internal Worker Endpoints

#### Cluster Health & Status
```bash
# Health checks
curl -s http://localhost:8000/health
curl -s http://localhost:8001/health

# Cluster status (workers, alive count, under-replicated chunks)
curl -s http://localhost:8000/admin/status | python3 -m json.tool

# Detailed status (lists every chunk, replicas, and alive flags)
curl -s "http://localhost:8000/admin/status?detail=true" | python3 -m json.tool
```

#### Internal Worker Chunk Operations (Port 8001 for Worker 1)
```bash
# Check chunk on worker disk
curl -s http://localhost:8001/internal/chunks/{chunk_id}

# Compute fresh SHA-256 and size from worker disk
curl -s http://localhost:8001/internal/chunks/{chunk_id}/checksum

# Delete chunk from worker
curl -X DELETE http://localhost:8001/internal/chunks/{chunk_id}
```

---

## 5. Running the Demonstration Script

A self-contained Bash script executes the six required demonstrations with explanatory banners and narrating pauses:

```bash
# Fresh run
docker compose down -v && docker compose up --build -d && ./scripts/demo.sh
```

### Demonstrations Included:
1. **Upload & Download**: Uploads a sample file, downloads it, and verifies matching SHA-256 hashes.
2. **Chunking (>1 MB)**: Uploads a 2.5 MB file, displays the resulting 3 chunks in metadata, and lists `/data/chunks` across each worker container.
3. **Replication**: Verifies that every chunk has exactly 2 replicas located on 2 distinct worker nodes.
4. **Metadata Updates**: Uploads an object with custom metadata, verifies it via GET, applies a PATCH (updating, adding, and deleting keys), and verifies via GET without any byte re-upload.
5. **Node Failure & Auto-Repair**: Stops `worker2`, shows `admin/status` with worker marked `DEAD`, demonstrates uninterrupted downloads, waits for the background repair loop to restore replication, restarts `worker2`, and confirms cluster recovery.
6. **Data Corruption & Read-Repair**: Corrupts a byte on a worker using `scripts/corrupt_chunk.sh`, downloads the file, proves the controller detected corruption, served good bytes from the alternate replica, repaired the bad chunk, and logs `corruption_detected` and `corruption_repaired`.

---

## 6. Simulating Node Failure & Corruption

### Simulating Worker Crash & Recovery
```bash
# 1. Stop worker2
docker compose stop worker2

# 2. Inspect controller failure detector (DEAD_AFTER = 10s)
curl -s http://localhost:8000/admin/status | python3 -m json.tool

# 3. Downloads continue working seamlessly from alive replica
curl -s http://localhost:8000/api/v1/objects/{object_id} > /tmp/out.bin

# 4. Recover worker
docker compose start worker2
```

### Simulating Chunk Bit-Flip Corruption
```bash
# Flips byte at offset 0 of the chunk file inside the worker container using dd conv=notrunc:
./scripts/corrupt_chunk.sh worker1 <chunk_id>

# Check worker reports bad hash:
curl -s http://localhost:8001/internal/chunks/<chunk_id>/checksum

# Downloading the object triggers transparent read-repair:
curl -s http://localhost:8000/api/v1/objects/{object_id} > /tmp/out.bin

# View controller read-repair log lines:
docker compose logs controller | grep -E "corruption_detected|corruption_repaired"
```

---

## 7. Key Design Decisions

- **Chunk Size (`CHUNK_SIZE = 1 MB`)**:
  Files are chunked during streaming. 1 MB chunks allow low-memory streaming uploads/downloads without buffering entire files in memory, while avoiding excessive SQLite metadata overhead for typical files.
- **Replication Factor (`REPLICATION = 2`)**:
  Provides single-node fault tolerance. Tolerates any single worker dying without downtime or data loss, with a 2x storage overhead.
- **Placement Policy**:
  Chunks are placed on the 2 ALIVE workers with the fewest active replicas (least-loaded), breaking ties randomly. Guarantees that chunks are never placed on the same worker twice.
- **Failure Detection (`HEARTBEAT_INTERVAL = 3s`, `DEAD_AFTER = 10s`)**:
  Workers send periodic heartbeats to the controller. If a heartbeat is missing for >10s, the background monitor thread marks the worker `DEAD` and logs `worker_dead`.
- **Integrity Validation & Read-Repair**:
  Every chunk upload includes an `X-Checksum-SHA256` header validated atomically by the worker. On download, if a chunk SHA-256 does not match `chunks.checksum`, the controller logs `corruption_detected`, fetches from the good replica, overwrites the corrupted replica via PUT, and logs `corruption_repaired`.
- **Active Scrubber & Pruning (`SCRUB_INTERVAL = 30s`)**:
  Every 30 seconds, the scrubber queries `/internal/chunks/{id}/checksum` for every replica on ALIVE workers. Any missing or mismatched chunk triggers replica row deletion so the repair loop restores it. In addition, when recovered nodes create surplus replicas (>2), the scrubber prunes excess replicas.
- **Worker Reconcile on Startup**:
  When a worker boots or re-registers, it reports its disk inventory. The controller deletes orphan chunks (chunks not in the DB) in the background and re-syncs missing replica rows for complete objects.

---

## 8. Known Limitations

- **Single Controller (Single Point of Failure)**: The controller manages all metadata and coordinates traffic. If the controller node goes down, the entire cluster becomes unavailable. Production systems (like GFS/HDFS/Ceph) mitigate this using Raft or Paxos leader election with multiple metadata replicas.
- **Synchronous Chunk Transfer**: Chunks are transferred sequentially via HTTP to worker pairs rather than pipelined in parallel or split via Reed-Solomon erasure coding.
- **Local SQLite Storage**: Metadata lives in an SQLite file on the controller volume. While fast and ACID-compliant via WAL mode and a global thread lock, it does not scale horizontally.
