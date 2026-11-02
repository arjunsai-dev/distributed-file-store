import sqlite3
import threading

DB_PATH = "/data/controller.db"
_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with _lock:
        conn = _connect()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS objects (
                    object_id TEXT PRIMARY KEY,
                    name      TEXT NOT NULL,
                    size      INTEGER,
                    content_type TEXT,
                    created_at TEXT,
                    modified_at TEXT,
                    checksum  TEXT,
                    num_chunks INTEGER,
                    custom_meta TEXT DEFAULT '{}',
                    status    TEXT NOT NULL DEFAULT 'UPLOADING'
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id  TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    idx       INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    checksum  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replicas (
                    chunk_id  TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    PRIMARY KEY (chunk_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id      TEXT PRIMARY KEY,
                    url            TEXT NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    status         TEXT NOT NULL DEFAULT 'ALIVE'
                );
            """)
            conn.commit()
        finally:
            conn.close()


def execute(sql: str, params: tuple = (), *, commit: bool = False):
    """Run a single SQL statement under the global lock. Returns list of Row."""
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(sql, params)
            if commit:
                conn.commit()
            rows = cur.fetchall()
            return rows
        finally:
            conn.close()


def execute_many(statements: list[tuple[str, tuple]], *, commit: bool = True):
    """Run multiple statements in one transaction under the global lock."""
    with _lock:
        conn = _connect()
        try:
            for sql, params in statements:
                conn.execute(sql, params)
            if commit:
                conn.commit()
        finally:
            conn.close()
