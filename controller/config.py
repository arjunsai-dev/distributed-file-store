import os

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1048576))
REPLICATION = int(os.environ.get("REPLICATION", 2))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 3))
DEAD_AFTER = int(os.environ.get("DEAD_AFTER", 10))
REPAIR_INTERVAL = int(os.environ.get("REPAIR_INTERVAL", 5))
SCRUB_INTERVAL = int(os.environ.get("SCRUB_INTERVAL", 30))
