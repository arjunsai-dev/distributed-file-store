import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, File, Form, Response, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

import db
import placement
import repair
from config import CHUNK_SIZE, REPLICATION

app = FastAPI()


# --------------- startup ---------------

@app.on_event("startup")
def on_startup():
    db.init_db()
    repair.start_monitor()


# --------------- models ---------------

class RegisterRequest(BaseModel):
    worker_id: str
    url: str
    chunks: list[dict] = []


class HeartbeatRequest(BaseModel):
    worker_id: str


# --------------- internal worker endpoints ---------------

@app.post("/internal/workers/register")
def register_worker(req: RegisterRequest):
    now = time.time()
    # Upsert worker, mark ALIVE
    existing = db.execute(
        "SELECT worker_id, status FROM workers WHERE worker_id = ?",
        (req.worker_id,),
    )
    if existing:
        old_status = existing[0]["status"]
        db.execute(
            "UPDATE workers SET url = ?, last_heartbeat = ?, status = 'ALIVE' "
            "WHERE worker_id = ?",
            (req.url, now, req.worker_id),
            commit=True,
        )
        if old_status == "DEAD":
            print(
                f"[controller] event=worker_alive worker_id={req.worker_id}"
            )
    else:
        db.execute(
            "INSERT INTO workers (worker_id, url, last_heartbeat, status) "
            "VALUES (?, ?, ?, 'ALIVE')",
            (req.worker_id, req.url, now),
            commit=True,
        )
    print(f"[controller] event=worker_registered worker_id={req.worker_id}")
    return {"status": "ok"}


@app.post("/internal/workers/heartbeat")
def heartbeat(req: HeartbeatRequest):
    rows = db.execute(
        "SELECT worker_id, status FROM workers WHERE worker_id = ?",
        (req.worker_id,),
    )
    if not rows:
        return Response(
            status_code=404,
            media_type="application/json",
            content='{"error":{"code":"UNKNOWN_WORKER","message":"worker not registered"}}',
        )
    old_status = rows[0]["status"]
    now = time.time()
    db.execute(
        "UPDATE workers SET last_heartbeat = ?, status = 'ALIVE' "
        "WHERE worker_id = ?",
        (now, req.worker_id),
        commit=True,
    )
    if old_status == "DEAD":
        print(f"[controller] event=worker_alive worker_id={req.worker_id}")
    return {"status": "ok"}


@app.get("/internal/workers/{worker_id}/chunks")
def get_worker_chunks(worker_id: str):
    rows = db.execute(
        "SELECT worker_id FROM workers WHERE worker_id = ?",
        (worker_id,),
    )
    if not rows:
        return Response(
            status_code=404,
            media_type="application/json",
            content='{"error":{"code":"UNKNOWN_WORKER","message":"worker not registered"}}',
        )
    chunks = db.execute(
        "SELECT r.chunk_id, c.object_id, c.idx, c.size, c.checksum "
        "FROM replicas r JOIN chunks c ON r.chunk_id = c.chunk_id "
        "WHERE r.worker_id = ?",
        (worker_id,),
    )
    return {
        "worker_id": worker_id,
        "chunks": [dict(row) for row in chunks],
    }


# --------------- admin ---------------

@app.get("/admin/status")
def admin_status(detail: bool = False):
    workers = db.execute(
        "SELECT w.worker_id, w.status, "
        "  (SELECT COUNT(*) FROM replicas r WHERE r.worker_id = w.worker_id) AS chunk_count "
        "FROM workers w",
    )

    # Count under-replicated: chunks with live replicas < REPLICATION
    under = db.execute(
        "SELECT COUNT(*) AS cnt FROM ("
        "  SELECT c.chunk_id, "
        "    SUM(CASE WHEN w.status = 'ALIVE' THEN 1 ELSE 0 END) AS live "
        "  FROM chunks c "
        "  LEFT JOIN replicas r ON c.chunk_id = r.chunk_id "
        "  LEFT JOIN workers w ON r.worker_id = w.worker_id "
        "  GROUP BY c.chunk_id "
        "  HAVING live < ?"
        ")",
        (REPLICATION,),
    )
    under_count = under[0]["cnt"] if under else 0

    result = {
        "workers": [
            {
                "worker_id": row["worker_id"],
                "status": row["status"],
                "chunk_count": row["chunk_count"],
            }
            for row in workers
        ],
        "under_replicated_chunks": under_count,
    }

    if detail:
        all_chunks = db.execute(
            "SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum "
            "FROM chunks c ORDER BY c.chunk_id",
        )
        chunk_list = []
        for ch in all_chunks:
            replicas = db.execute(
                "SELECT r.worker_id, w.status "
                "FROM replicas r JOIN workers w ON r.worker_id = w.worker_id "
                "WHERE r.chunk_id = ?",
                (ch["chunk_id"],),
            )
            chunk_list.append({
                "chunk_id": ch["chunk_id"],
                "object_id": ch["object_id"],
                "index": ch["idx"],
                "size": ch["size"],
                "checksum": ch["checksum"],
                "replicas": [
                    {"worker_id": r["worker_id"], "alive": r["status"] == "ALIVE"}
                    for r in replicas
                ],
            })
        result["chunks"] = chunk_list

    return result


# --------------- health ---------------

@app.get("/health")
def health():
    return {"status": "ok", "service": "controller"}


# --------------- public api (/api/v1) ---------------

def _read_exact(file_obj, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        packet = file_obj.read(n - len(data))
        if not packet:
            break
        data.extend(packet)
    return bytes(data)


@app.post("/api/v1/objects", status_code=201)
@app.post("/objects", status_code=201)
def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    # Reject before inserting object row if fewer than 2 workers alive
    alive_workers = db.execute("SELECT COUNT(*) AS cnt FROM workers WHERE status = 'ALIVE'")
    if not alive_workers or alive_workers[0]["cnt"] < REPLICATION:
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "SERVICE_UNAVAILABLE", "message": "fewer than 2 workers alive"}},
        )

    # Validate metadata JSON if provided
    custom_meta = {}
    if metadata is not None and metadata.strip() != "":
        try:
            parsed = json.loads(metadata)
            if not isinstance(parsed, dict):
                return JSONResponse(
                    status_code=400,
                    content={"error": {"code": "INVALID_METADATA", "message": "metadata must be a JSON object"}},
                )
            custom_meta = parsed
        except Exception:
            return JSONResponse(
                status_code=400,
                content={"error": {"code": "INVALID_METADATA", "message": "invalid metadata JSON"}},
            )

    # Reject before inserting object row if file size is 0
    first_chunk = _read_exact(file.file, CHUNK_SIZE)
    if not first_chunk or len(first_chunk) == 0:
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "EMPTY_FILE", "message": "file is empty"}},
        )

    object_id = str(uuid.uuid4())
    filename = file.filename or "unnamed"
    content_type = file.content_type or "application/octet-stream"
    created_at = datetime.now(timezone.utc).isoformat()

    # Insert object(status=UPLOADING)
    db.execute(
        """
        INSERT INTO objects (object_id, name, size, content_type, created_at, modified_at, checksum, num_chunks, custom_meta, status)
        VALUES (?, ?, 0, ?, ?, ?, '', 0, ?, 'UPLOADING')
        """,
        (object_id, filename, content_type, created_at, created_at, json.dumps(custom_meta)),
        commit=True,
    )

    placed_tracker = []
    total_size = 0
    object_hasher = hashlib.sha256()
    idx = 0
    current_chunk = first_chunk

    try:
        while current_chunk:
            chunk_size = len(current_chunk)
            total_size += chunk_size
            object_hasher.update(current_chunk)
            chunk_checksum = hashlib.sha256(current_chunk).hexdigest()
            chunk_id = f"{object_id}_{idx}"

            # Place chunk on 2 workers
            placed_workers = placement.place_chunk(chunk_id, current_chunk, chunk_checksum, placed_tracker)

            # Insert chunk + replica rows
            db.execute(
                "INSERT INTO chunks (chunk_id, object_id, idx, size, checksum) VALUES (?, ?, ?, ?, ?)",
                (chunk_id, object_id, idx, chunk_size, chunk_checksum),
                commit=True,
            )
            for w in placed_workers:
                db.execute(
                    "INSERT INTO replicas (chunk_id, worker_id) VALUES (?, ?)",
                    (chunk_id, w["worker_id"]),
                    commit=True,
                )

            idx += 1
            current_chunk = _read_exact(file.file, CHUNK_SIZE)

        # After the last chunk set size, checksum, num_chunks, status=COMPLETE
        object_checksum = object_hasher.hexdigest()
        num_chunks = idx
        modified_at = datetime.now(timezone.utc).isoformat()

        db.execute(
            """
            UPDATE objects
            SET size = ?, checksum = ?, num_chunks = ?, status = 'COMPLETE', modified_at = ?
            WHERE object_id = ?
            """,
            (total_size, object_checksum, num_chunks, modified_at, object_id),
            commit=True,
        )

        return JSONResponse(
            status_code=201,
            content={
                "object_id": object_id,
                "name": filename,
                "size": total_size,
                "content_type": content_type,
                "created_at": created_at,
                "modified_at": modified_at,
                "checksum": object_checksum,
                "num_chunks": num_chunks,
                "custom_meta": custom_meta,
                "status": "COMPLETE",
            },
        )
    except Exception as exc:
        print(f"[controller] event=upload_failed object_id={object_id} error={exc}")
        placement.rollback_upload(object_id, placed_tracker)
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "SERVICE_UNAVAILABLE", "message": f"upload failed: {exc}"}},
        )
    finally:
        try:
            file.file.close()
        except Exception:
            pass


# --------------- helpers for download ---------------

def _get_object_or_error(object_id: str):
    """Return (obj_row, None) or (None, JSONResponse)."""
    rows = db.execute(
        "SELECT * FROM objects WHERE object_id = ?",
        (object_id,),
    )
    if not rows:
        return None, JSONResponse(
            status_code=404,
            content={"error": {"code": "NOT_FOUND", "message": "object not found"}},
        )
    obj = rows[0]
    if obj["status"] == "UPLOADING":
        return None, JSONResponse(
            status_code=409,
            content={"error": {"code": "CONFLICT", "message": "object is still uploading"}},
        )
    if obj["status"] == "DELETING":
        return None, JSONResponse(
            status_code=404,
            content={"error": {"code": "NOT_FOUND", "message": "object not found"}},
        )
    return obj, None


def _fetch_chunk_verified(chunk_row) -> bytes | None:
    """
    Try each ALIVE replica for this chunk; return verified bytes or None.
    """
    replicas = db.execute(
        "SELECT r.worker_id, w.url, w.status "
        "FROM replicas r JOIN workers w ON r.worker_id = w.worker_id "
        "WHERE r.chunk_id = ? AND w.status = 'ALIVE'",
        (chunk_row["chunk_id"],),
    )
    for rep in replicas:
        try:
            with httpx.Client(timeout=10.0) as client:
                res = client.get(f"{rep['url']}/internal/chunks/{chunk_row['chunk_id']}")
            if res.status_code != 200:
                continue
            data = res.content
            if hashlib.sha256(data).hexdigest() == chunk_row["checksum"]:
                return data
            else:
                print(
                    f"[controller] event=corruption_detected "
                    f"chunk_id={chunk_row['chunk_id']} worker_id={rep['worker_id']}"
                )
        except Exception as exc:
            print(
                f"[controller] event=chunk_fetch_failed "
                f"chunk_id={chunk_row['chunk_id']} worker_id={rep['worker_id']} error={exc}"
            )
    return None


# --------------- Task 5: GET /api/v1/objects/{object_id} ---------------

@app.get("/api/v1/objects/{object_id}")
def download_object(object_id: str):
    obj, err = _get_object_or_error(object_id)
    if err:
        return err

    chunks = db.execute(
        "SELECT * FROM chunks WHERE object_id = ? ORDER BY idx",
        (object_id,),
    )
    if not chunks:
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "NO_VALID_REPLICA", "message": "no chunks found"}},
        )

    # Fetch chunk 0 BEFORE sending headers so a failure can return 503
    chunk0_data = _fetch_chunk_verified(chunks[0])
    if chunk0_data is None:
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "NO_VALID_REPLICA", "message": "cannot retrieve chunk 0"}},
        )

    def _stream():
        # Yield chunk 0 (already fetched)
        yield chunk0_data
        # Stream remaining chunks
        for ch in chunks[1:]:
            data = _fetch_chunk_verified(ch)
            if data is None:
                # Mid-stream failure — can't change status code now
                print(
                    f"[controller] event=download_chunk_failed "
                    f"chunk_id={ch['chunk_id']} object_id={object_id}"
                )
                return
            yield data

    content_type = obj["content_type"] or "application/octet-stream"
    filename = obj["name"] or "download"

    return StreamingResponse(
        _stream(),
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(obj["size"]),
        },
    )


# --------------- Task 6: metadata, list, delete ---------------

@app.get("/api/v1/objects/{object_id}/metadata")
def get_object_metadata(object_id: str):
    obj, err = _get_object_or_error(object_id)
    if err:
        return err

    chunks = db.execute(
        "SELECT chunk_id, idx, size, checksum FROM chunks WHERE object_id = ? ORDER BY idx",
        (object_id,),
    )
    chunk_list = []
    for ch in chunks:
        replicas = db.execute(
            "SELECT r.worker_id, w.status "
            "FROM replicas r JOIN workers w ON r.worker_id = w.worker_id "
            "WHERE r.chunk_id = ?",
            (ch["chunk_id"],),
        )
        chunk_list.append({
            "chunk_id": ch["chunk_id"],
            "index": ch["idx"],
            "size": ch["size"],
            "checksum": ch["checksum"],
            "replicas": [
                {"worker_id": r["worker_id"], "alive": r["status"] == "ALIVE"}
                for r in replicas
            ],
        })

    custom_meta = {}
    try:
        custom_meta = json.loads(obj["custom_meta"]) if obj["custom_meta"] else {}
    except Exception:
        pass

    return {
        "object_id": obj["object_id"],
        "name": obj["name"],
        "size": obj["size"],
        "content_type": obj["content_type"],
        "created_at": obj["created_at"],
        "modified_at": obj["modified_at"],
        "checksum": obj["checksum"],
        "num_chunks": obj["num_chunks"],
        "status": obj["status"],
        "custom_meta": custom_meta,
        "chunks": chunk_list,
    }


@app.patch("/api/v1/objects/{object_id}/metadata")
def patch_object_metadata(object_id: str, body: dict):
    obj, err = _get_object_or_error(object_id)
    if err:
        return err

    if not isinstance(body, dict):
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "INVALID_BODY", "message": "body must be a JSON object"}},
        )

    # Merge into custom_meta; null value deletes the key
    try:
        custom_meta = json.loads(obj["custom_meta"]) if obj["custom_meta"] else {}
    except Exception:
        custom_meta = {}

    for k, v in body.items():
        if v is None:
            custom_meta.pop(k, None)
        else:
            custom_meta[k] = v

    modified_at = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE objects SET custom_meta = ?, modified_at = ? WHERE object_id = ?",
        (json.dumps(custom_meta), modified_at, object_id),
        commit=True,
    )

    return {"status": "ok", "custom_meta": custom_meta, "modified_at": modified_at}


@app.get("/api/v1/objects")
def list_objects(limit: int = 100, offset: int = 0):
    rows = db.execute(
        "SELECT object_id, name, size, content_type "
        "FROM objects WHERE status = 'COMPLETE' "
        "ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )
    return [dict(r) for r in rows]


@app.delete("/api/v1/objects/{object_id}", status_code=204)
def delete_object(object_id: str):
    rows = db.execute(
        "SELECT * FROM objects WHERE object_id = ?",
        (object_id,),
    )
    if not rows:
        return JSONResponse(
            status_code=404,
            content={"error": {"code": "NOT_FOUND", "message": "object not found"}},
        )
    obj = rows[0]
    if obj["status"] == "UPLOADING":
        return JSONResponse(
            status_code=409,
            content={"error": {"code": "CONFLICT", "message": "object is still uploading"}},
        )

    # Set status=DELETING so concurrent reads see 404
    db.execute(
        "UPDATE objects SET status = 'DELETING' WHERE object_id = ?",
        (object_id,),
        commit=True,
    )

    # Get replicas on ALIVE workers to delete chunks from
    replica_info = db.execute(
        "SELECT r.chunk_id, w.worker_id, w.url "
        "FROM replicas r "
        "JOIN chunks c ON r.chunk_id = c.chunk_id "
        "JOIN workers w ON r.worker_id = w.worker_id "
        "WHERE c.object_id = ? AND w.status = 'ALIVE'",
        (object_id,),
    )

    # Remove DB rows first (so reads stop)
    db.execute_many([
        ("DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id = ?)", (object_id,)),
        ("DELETE FROM chunks WHERE object_id = ?", (object_id,)),
        ("DELETE FROM objects WHERE object_id = ?", (object_id,)),
    ], commit=True)

    # Then delete chunks from ALIVE workers
    with httpx.Client(timeout=5.0) as client:
        for rep in replica_info:
            try:
                client.delete(f"{rep['url']}/internal/chunks/{rep['chunk_id']}")
            except Exception as exc:
                print(
                    f"[controller] event=delete_chunk_failed "
                    f"chunk_id={rep['chunk_id']} worker_id={rep['worker_id']} error={exc}"
                )

    return Response(status_code=204)
