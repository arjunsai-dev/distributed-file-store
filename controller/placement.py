import httpx

import db
from config import REPLICATION


def get_alive_workers_by_load(exclude_worker_ids: set[str] | None = None) -> list[dict]:
    """
    Returns ALIVE workers ordered by number of replica rows held (ascending),
    with random tie-breaking.
    """
    rows = db.execute(
        """
        SELECT w.worker_id, w.url, COUNT(r.chunk_id) AS replica_count
        FROM workers w
        LEFT JOIN replicas r ON w.worker_id = r.worker_id
        WHERE w.status = 'ALIVE'
        GROUP BY w.worker_id
        ORDER BY replica_count ASC, RANDOM()
        """
    )
    exclude = exclude_worker_ids or set()
    return [dict(r) for r in rows if r["worker_id"] not in exclude]


def place_chunk(chunk_id: str, chunk_data: bytes, chunk_checksum: str, placed_tracker: list) -> list[dict]:
    """
    Places a chunk on REPLICATION distinct ALIVE workers.
    Retries on other alive workers on failure.
    Tracks successful placements in placed_tracker.
    Raises RuntimeError if REPLICATION targets cannot be placed.
    """
    successful = []
    tried = set()

    while len(successful) < REPLICATION:
        exclude = tried | {w["worker_id"] for w in successful}
        candidates = get_alive_workers_by_load(exclude_worker_ids=exclude)
        if not candidates:
            raise RuntimeError(f"Unable to place chunk {chunk_id}: insufficient alive workers")

        target = candidates[0]
        tried.add(target["worker_id"])

        try:
            with httpx.Client(timeout=5.0) as client:
                res = client.put(
                    f"{target['url']}/internal/chunks/{chunk_id}",
                    content=chunk_data,
                    headers={
                        "X-Checksum-SHA256": chunk_checksum,
                        "Content-Type": "application/octet-stream",
                    },
                )
                if res.status_code == 201:
                    successful.append(target)
                    placed_tracker.append({
                        "chunk_id": chunk_id,
                        "worker_id": target["worker_id"],
                        "url": target["url"],
                    })
                    print(f"[controller] event=chunk_placed chunk_id={chunk_id} worker_id={target['worker_id']}")
                else:
                    print(f"[controller] event=put_chunk_failed chunk_id={chunk_id} worker_id={target['worker_id']} status={res.status_code}")
        except Exception as exc:
            print(f"[controller] event=put_chunk_failed chunk_id={chunk_id} worker_id={target['worker_id']} error={exc}")

    return successful


def rollback_upload(object_id: str, placed_tracker: list):
    """
    Best-effort rollback: DELETE placed chunks from currently ALIVE workers
    and remove object/chunk/replica rows under lock.
    """
    with db._lock:
        alive_rows = db.execute("SELECT worker_id, url FROM workers WHERE status = 'ALIVE'")
        alive_map = {r["worker_id"]: r["url"] for r in alive_rows}

        with httpx.Client(timeout=3.0) as client:
            for item in placed_tracker:
                w_id = item["worker_id"]
                if w_id in alive_map:
                    url = item.get("url") or alive_map[w_id]
                    try:
                        client.delete(f"{url}/internal/chunks/{item['chunk_id']}")
                    except Exception as exc:
                        print(f"[controller] event=rollback_delete_failed worker_id={w_id} chunk_id={item['chunk_id']} error={exc}")

        db.execute_many([
            ("DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id = ?)", (object_id,)),
            ("DELETE FROM chunks WHERE object_id = ?", (object_id,)),
            ("DELETE FROM objects WHERE object_id = ?", (object_id,)),
        ], commit=True)
