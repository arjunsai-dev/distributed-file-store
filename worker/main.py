import hashlib
import os
import tempfile
import threading
import time

import httpx
from fastapi import Body, FastAPI, Header, Response

app = FastAPI()

WORKER_ID = os.environ.get("WORKER_ID", "unknown")
WORKER_URL = os.environ.get("WORKER_URL", "http://localhost:8000")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 3))
CHUNKS_DIR = "/data/chunks"

os.makedirs(CHUNKS_DIR, exist_ok=True)


# --------------- helpers ---------------

def _chunk_path(chunk_id: str) -> str:
    return os.path.join(CHUNKS_DIR, chunk_id)


def _inventory() -> list[dict]:
    """List all chunks on disk with their sizes."""
    items = []
    if os.path.isdir(CHUNKS_DIR):
        for name in os.listdir(CHUNKS_DIR):
            if name.startswith("tmp") or name.startswith("."):
                continue
            path = os.path.join(CHUNKS_DIR, name)
            if os.path.isfile(path):
                items.append({"chunk_id": name, "size": os.path.getsize(path)})
    return items


def _register() -> bool:
    """Try to register with the controller. Returns True on success."""
    try:
        payload = {
            "worker_id": WORKER_ID,
            "url": WORKER_URL,
            "chunks": _inventory(),
        }
        r = httpx.post(
            f"{CONTROLLER_URL}/internal/workers/register",
            json=payload,
            timeout=5,
        )
        if r.status_code < 300:
            print(f"[worker] event=registered worker_id={WORKER_ID}")
            return True
        print(f"[worker] event=register_failed worker_id={WORKER_ID} status={r.status_code}")
        return False
    except Exception as exc:
        print(f"[worker] event=register_failed worker_id={WORKER_ID} error={exc}")
        return False


# --------------- background threads ---------------

def _startup_register():
    """Retry registration until success (runs in a daemon thread)."""
    while True:
        if _register():
            return
        time.sleep(2)


def _heartbeat_loop():
    """Send heartbeats; re-register on 404."""
    # Wait a moment for initial registration attempt
    time.sleep(1)
    while True:
        try:
            r = httpx.post(
                f"{CONTROLLER_URL}/internal/workers/heartbeat",
                json={"worker_id": WORKER_ID},
                timeout=5,
            )
            if r.status_code == 404:
                print(f"[worker] event=heartbeat_404 worker_id={WORKER_ID}")
                _register()
        except Exception as exc:
            print(f"[worker] event=heartbeat_failed worker_id={WORKER_ID} error={exc}")
        time.sleep(HEARTBEAT_INTERVAL)


@app.on_event("startup")
def on_startup():
    threading.Thread(target=_startup_register, daemon=True).start()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()


# --------------- chunk endpoints ---------------

@app.put("/internal/chunks/{chunk_id}", status_code=201)
def put_chunk(
    chunk_id: str,
    body: bytes = Body(...),
    x_checksum_sha256: str = Header(""),
):
    checksum_header = x_checksum_sha256

    computed = hashlib.sha256(body).hexdigest()
    if computed != checksum_header:
        return Response(
            status_code=400,
            media_type="application/json",
            content='{"error":{"code":"CHECKSUM_MISMATCH","message":"SHA-256 mismatch"}}',
        )

    path = _chunk_path(chunk_id)
    fd, tmp = tempfile.mkstemp(dir=CHUNKS_DIR)
    try:
        try:
            os.write(fd, body)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise

    print(f"[worker] event=chunk_stored worker_id={WORKER_ID} chunk_id={chunk_id} size={len(body)}")
    return Response(status_code=201)


@app.get("/internal/chunks/{chunk_id}")
def get_chunk(chunk_id: str):
    path = _chunk_path(chunk_id)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except (FileNotFoundError, IsADirectoryError):
        return Response(
            status_code=404,
            media_type="application/json",
            content='{"error":{"code":"CHUNK_NOT_FOUND","message":"chunk not found"}}',
        )
    return Response(content=data, media_type="application/octet-stream")


@app.delete("/internal/chunks/{chunk_id}", status_code=204)
def delete_chunk(chunk_id: str):
    path = _chunk_path(chunk_id)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    return Response(status_code=204)


@app.get("/internal/chunks/{chunk_id}/checksum")
def get_checksum(chunk_id: str):
    path = _chunk_path(chunk_id)
    h = hashlib.sha256()
    size = 0
    try:
        with open(path, "rb") as f:
            while True:
                block = f.read(65536)
                if not block:
                    break
                h.update(block)
                size += len(block)
    except (FileNotFoundError, IsADirectoryError):
        return Response(
            status_code=404,
            media_type="application/json",
            content='{"error":{"code":"CHUNK_NOT_FOUND","message":"chunk not found"}}',
        )
    return {"sha256": h.hexdigest(), "size": size}


@app.get("/health")
def health():
    return {"status": "ok", "service": "worker", "worker_id": WORKER_ID}
