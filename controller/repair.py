import threading
import time

import db
from config import DEAD_AFTER, REPAIR_INTERVAL


def _monitor_loop():
    """Every REPAIR_INTERVAL, mark workers DEAD/ALIVE based on heartbeat."""
    while True:
        time.sleep(REPAIR_INTERVAL)
        try:
            now = time.time()
            cutoff = now - DEAD_AFTER

            # Workers that should be DEAD
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

        except Exception as exc:
            print(f"[controller] event=monitor_error error={exc}")


def start_monitor():
    """Start the background monitor thread."""
    t = threading.Thread(target=_monitor_loop, daemon=True)
    t.start()
