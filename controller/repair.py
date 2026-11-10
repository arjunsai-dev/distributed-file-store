import hashlib
import threading
import time

import httpx

import db
import placement
from config import DEAD_AFTER, REPAIR_INTERVAL, REPLICATION, SCRUB_INTERVAL

_repair_lock = threading.Lock()

_repair_lock = threading.Lock()


def repair_under_replicated():
    """
    Steps 2-5 of the REPAIR LOOP in SPEC.md:
    - Finds chunks with live replicas < REPLICATION.
    - If 0 live replicas, logs and skips.
    - If >= 1 live source, picks an ALIVE worker not holding it.
    - Verifies source replica with GET .../checksum.
    - GETs chunk from source, verifies SHA-256 against chunks.checksum.
    - PUTs to target, inserts replica row.
    - Logs replica_lost, repair_start, repair_done.
    """
    if not _repair_lock.acquire(blocking=False):
        return

    try:
        # Find under-replicated chunks for COMPLETE objects
        under_replicated = db.execute(
            """
            SELECT c.chunk_id, c.checksum,
                   SUM(CASE WHEN w.status = 'ALIVE' THEN 1 ELSE 0 END) AS live_count
            FROM chunks c
            JOIN objects o ON c.object_id = o.object_id
            LEFT JOIN replicas r ON c.chunk_id = r.chunk_id
            LEFT JOIN workers w ON r.worker_id = w.worker_id
            WHERE o.status = 'COMPLETE'
            GROUP BY c.chunk_id
            HAVING live_count < ?
            """,
            (REPLICATION,),
        )

        for row in under_replicated:
            chunk_id = row["chunk_id"]
            expected_checksum = row["checksum"]
            live_count = row["live_count"]

            print(f"[controller] event=replica_lost chunk_id={chunk_id} live_replicas={live_count}")

            if live_count == 0:
                print(f"[controller] event=repair_skipped chunk_id={chunk_id} reason=no_live_replicas")
                continue

            needed = REPLICATION - live_count
            for _ in range(needed):
                # Workers already holding this chunk (ALIVE or DEAD)
                held_rows = db.execute(
                    "SELECT worker_id FROM replicas WHERE chunk_id = ?",
                    (chunk_id,),
                )
                held_worker_ids = {r["worker_id"] for r in held_rows}

                # Candidates: ALIVE workers not holding this chunk
                candidates = placement.get_alive_workers_by_load(exclude_worker_ids=held_worker_ids)
                if not candidates:
                    print(f"[controller] event=repair_skipped chunk_id={chunk_id} reason=no_target_available")
                    break

                # Find live sources
                sources = db.execute(
                    """
                    SELECT r.worker_id, w.url
                    FROM replicas r
                    JOIN workers w ON r.worker_id = w.worker_id
                    WHERE r.chunk_id = ? AND w.status = 'ALIVE'
                    """,
                    (chunk_id,),
                )
                if not sources:
                    print(f"[controller] event=repair_skipped chunk_id={chunk_id} reason=no_live_source")
                    break

                # Verify and fetch from a live source
                chunk_bytes = None
                valid_source = None

                for src in sources:
                    src_id = src["worker_id"]
                    src_url = src["url"]

                    # Step 3: verify source replica with GET .../checksum
                    try:
                        with httpx.Client(timeout=5.0) as client:
                            r_chk = client.get(f"{src_url}/internal/chunks/{chunk_id}/checksum")
                        if r_chk.status_code != 200:
                            print(f"[controller] event=source_checksum_failed chunk_id={chunk_id} worker_id={src_id} status={r_chk.status_code}")
                            continue
                        chk_data = r_chk.json()
                        if chk_data.get("sha256") != expected_checksum:
                            print(f"[controller] event=corruption_detected chunk_id={chunk_id} worker_id={src_id}")
                            continue
                    except Exception as exc:
                        print(f"[controller] event=source_checksum_error chunk_id={chunk_id} worker_id={src_id} error={exc}")
                        continue

                    # Step 4: GET from source and verify SHA-256
                    try:
                        with httpx.Client(timeout=10.0) as client:
                            r_get = client.get(f"{src_url}/internal/chunks/{chunk_id}")
                        if r_get.status_code != 200:
                            print(f"[controller] event=source_get_failed chunk_id={chunk_id} worker_id={src_id} status={r_get.status_code}")
                            continue
                        data = r_get.content
                        if hashlib.sha256(data).hexdigest() != expected_checksum:
                            print(f"[controller] event=corruption_detected chunk_id={chunk_id} worker_id={src_id}")
                            continue

                        chunk_bytes = data
                        valid_source = src
                        break
                    except Exception as exc:
                        print(f"[controller] event=source_get_error chunk_id={chunk_id} worker_id={src_id} error={exc}")
                        continue

                if chunk_bytes is None or valid_source is None:
                    print(f"[controller] event=repair_skipped chunk_id={chunk_id} reason=all_sources_failed")
                    break

                # Try putting to target candidates
                repair_succeeded = False
                for target in candidates:
                    target_id = target["worker_id"]
                    target_url = target["url"]

                    print(f"[controller] event=repair_start chunk_id={chunk_id} source={valid_source['worker_id']} target={target_id}")

                    try:
                        with httpx.Client(timeout=10.0) as client:
                            r_put = client.put(
                                f"{target_url}/internal/chunks/{chunk_id}",
                                content=chunk_bytes,
                                headers={
                                    "X-Checksum-SHA256": expected_checksum,
                                    "Content-Type": "application/octet-stream",
                                },
                            )
                        if r_put.status_code == 201:
                            db.execute(
                                "INSERT OR IGNORE INTO replicas (chunk_id, worker_id) VALUES (?, ?)",
                                (chunk_id, target_id),
                                commit=True,
                            )
                            print(f"[controller] event=repair_done chunk_id={chunk_id} target={target_id}")
                            repair_succeeded = True
                            break
                        else:
                            print(f"[controller] event=target_put_failed chunk_id={chunk_id} target={target_id} status={r_put.status_code}")
                    except Exception as exc:
                        print(f"[controller] event=target_put_error chunk_id={chunk_id} target={target_id} error={exc}")

                if not repair_succeeded:
                    break

    except Exception as exc:
        print(f"[controller] event=repair_error error={exc}")
    finally:
        _repair_lock.release()


def _monitor_loop():
    """Every REPAIR_INTERVAL, mark workers DEAD and run repair."""
    while True:
        time.sleep(REPAIR_INTERVAL)
        try:
            now = time.time()
            cutoff = now - DEAD_AFTER

            # Step 1: Workers with last_heartbeat older than DEAD_AFTER -> DEAD
            newly_dead = db.execute(
                "SELECT worker_id FROM workers "
                "WHERE status = 'ALIVE' AND last_heartbeat < ?",
                (cutoff,),
            )
            for row in newly_dead:
                db.execute(
                    "UPDATE workers SET status = 'DEAD' WHERE worker_id = ?",
                    (row["worker_id"],),
                    commit=True,
                )
                print(
                    f"[controller] event=worker_dead worker_id={row['worker_id']}"
                )

            # Steps 2-5: Repair under-replicated chunks
            repair_under_replicated()

        except Exception as exc:
            print(f"[controller] event=monitor_error error={exc}")


def start_monitor():
    """Start the background monitor thread."""
    t = threading.Thread(target=_monitor_loop, daemon=True)
    t.start()


# --------------- SCRUBBER ---------------

def _scrub_loop():
    """Every SCRUB_INTERVAL: check every replica's checksum on disk.
    Mismatch or 404 → delete replica row (repair loop will restore it).
    """
    while True:
        time.sleep(SCRUB_INTERVAL)
        try:
            replicas = db.execute(
                """
                SELECT r.chunk_id, r.worker_id, w.url, c.checksum
                FROM replicas r
                JOIN workers w ON r.worker_id = w.worker_id
                JOIN chunks c ON r.chunk_id = c.chunk_id
                WHERE w.status = 'ALIVE'
                """
            )
            for rep in replicas:
                chunk_id = rep["chunk_id"]
                worker_id = rep["worker_id"]
                url = rep["url"]
                expected = rep["checksum"]
                try:
                    with httpx.Client(timeout=5.0) as client:
                        res = client.get(f"{url}/internal/chunks/{chunk_id}/checksum")
                    if res.status_code == 404:
                        db.execute(
                            "DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?",
                            (chunk_id, worker_id), commit=True,
                        )
                        print(
                            f"[controller] event=corruption_detected "
                            f"chunk_id={chunk_id} worker_id={worker_id} reason=missing"
                        )
                        continue
                    if res.status_code != 200:
                        continue
                    actual = res.json().get("sha256", "")
                    if actual != expected:
                        db.execute(
                            "DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?",
                            (chunk_id, worker_id), commit=True,
                        )
                        print(
                            f"[controller] event=corruption_detected "
                            f"chunk_id={chunk_id} worker_id={worker_id} reason=checksum_mismatch"
                        )
                except Exception as exc:
                    print(
                        f"[controller] event=scrub_error "
                        f"chunk_id={chunk_id} worker_id={worker_id} error={exc}"
                    )

            # Prune excess replicas so no chunk exceeds REPLICATION live copies
            over_replicated = db.execute(
                """
                SELECT c.chunk_id, COUNT(r.worker_id) AS live_count
                FROM chunks c
                JOIN replicas r ON c.chunk_id = r.chunk_id
                JOIN workers w ON r.worker_id = w.worker_id
                WHERE w.status = 'ALIVE'
                GROUP BY c.chunk_id
                HAVING live_count > ?
                """,
                (REPLICATION,),
            )
            for row in over_replicated:
                cid = row["chunk_id"]
                excess_count = row["live_count"] - REPLICATION
                excess_reps = db.execute(
                    """
                    SELECT r.worker_id, w.url
                    FROM replicas r
                    JOIN workers w ON r.worker_id = w.worker_id
                    WHERE r.chunk_id = ? AND w.status = 'ALIVE'
                    ORDER BY r.worker_id DESC
                    LIMIT ?
                    """,
                    (cid, excess_count),
                )
                for rep in excess_reps:
                    try:
                        with httpx.Client(timeout=5.0) as client:
                            client.delete(f"{rep['url']}/internal/chunks/{cid}")
                    except Exception as exc:
                        print(f"[controller] event=excess_delete_failed chunk_id={cid} worker_id={rep['worker_id']} error={exc}")
                    db.execute(
                        "DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?",
                        (cid, rep["worker_id"]),
                        commit=True,
                    )
                    print(f"[controller] event=excess_replica_removed chunk_id={cid} worker_id={rep['worker_id']}")
        except Exception as exc:
            print(f"[controller] event=scrubber_error error={exc}")


def start_scrubber():
    """Start the background scrubber thread."""
    t = threading.Thread(target=_scrub_loop, daemon=True)
    t.start()
