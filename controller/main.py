import time

from fastapi import FastAPI, Response
from pydantic import BaseModel

import db
import repair
from config import REPLICATION

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
