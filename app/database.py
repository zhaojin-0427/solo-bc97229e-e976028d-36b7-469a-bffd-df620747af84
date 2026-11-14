"""SQLite connection factories and schema management.

Two physical databases are deliberately used:

* ``business.db`` holds the client-facing data (derivation relations, sample
  groups, data-version bookkeeping). Upload transactions only touch this file
  and stay short.
* ``check.db`` holds verification runs, progress and conflict details. The
  background verifier writes there, so its long-running work never holds a
  write transaction on the business database.

Both files live on a shared volume so the two API processes see the same
state. Every state change on the business side creates a new monotonically
increasing *data version*; verifier conclusions are bound to the exact
version they examined, and stale conclusions can never overwrite a newer
state.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import settings

# --- pragmas ---------------------------------------------------------------

# WAL gives readers (verifier, download, GET endpoints) consistent snapshots
# without blocking the single writer, and survives across processes.
_BUSY_TIMEOUT_MS = 15_000


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # Bound the page cache so even million-row working tables cannot push the
    # process memory up without limit; the graph/search state stays on disk.
    conn.execute("PRAGMA cache_size=-20000")  # ~20 MB soft cache cap


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=_BUSY_TIMEOUT_MS / 1000)
    _apply_pragmas(conn)
    return conn


@contextmanager
def transaction(db_path: str) -> Iterator[sqlite3.Connection]:
    """Short write transaction. Commits on success, rolls back on error.

    ``BEGIN IMMEDIATE`` acquires the reserved write lock up front, so two
    processes contending for the same run can never both read it as
    claimable (a deferred transaction would allow that race).
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- schema ----------------------------------------------------------------

BUSINESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Append-only derivation edges. Natural key (parent_sample, child_sample)
-- makes duplicate uploads merge into the same row ("重复合一").
CREATE TABLE IF NOT EXISTS derivation_edge (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_sample TEXT NOT NULL,
    child_sample  TEXT NOT NULL,
    version       INTEGER NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(parent_sample, child_sample)
);

-- Whole-replacement group assignments. Only rows of the current batch
-- (batch_id = active batch) are the effective grouping.
CREATE TABLE IF NOT EXISTS sample_group (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_no  TEXT NOT NULL,
    grp        TEXT NOT NULL,
    batch_id   INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(sample_no, batch_id)
);

-- One row per effective grouping batch.
CREATE TABLE IF NOT EXISTS group_batch (
    batch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    version       INTEGER NOT NULL,
    sample_count  INTEGER NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Monotonic data-version ledger. Every committed mutation appends one row.
CREATE TABLE IF NOT EXISTS data_version (
    version     INTEGER PRIMARY KEY AUTOINCREMENT,
    change_type TEXT NOT NULL,            -- relation | groups
    detail      TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Acceptance ("放行") state. At most one accepted version at a time; any new
-- version invalidates the previous acceptance (status superseded).
CREATE TABLE IF NOT EXISTS acceptance (
    accepted_version INTEGER PRIMARY KEY,
    accepted_at      TEXT NOT NULL DEFAULT (datetime('now')),
    run_id           INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'superseded'))
);

-- Acceptance snapshots available for download. The file is frozen at
-- acceptance time, so later uploads can never bleed into a download.
CREATE TABLE IF NOT EXISTS release_artifact (
    accepted_version INTEGER PRIMARY KEY,
    file_path        TEXT NOT NULL,
    row_count        INTEGER NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

CHECK_SCHEMA = """
CREATE TABLE IF NOT EXISTS verifier_run (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    data_version        INTEGER NOT NULL,
    status              TEXT NOT NULL
                        CHECK (status IN
                               ('queued', 'running', 'passed',
                                'failed', 'superseded')),
    queued_at           TEXT NOT NULL DEFAULT (datetime('now')),
    started_at          TEXT,
    finished_at         TEXT,
    progress_pct        REAL NOT NULL DEFAULT 0,
    vertices_total      INTEGER,
    vertices_processed  INTEGER NOT NULL DEFAULT 0,
    edges_total         INTEGER,
    components_total    INTEGER,
    conflicts_total     INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT,
    attempts            INTEGER NOT NULL DEFAULT 0,
    worker_id           TEXT,
    heartbeat_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_version ON verifier_run(data_version);
CREATE INDEX IF NOT EXISTS idx_run_status  ON verifier_run(status);

CREATE TABLE IF NOT EXISTS verifier_conflict (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         INTEGER NOT NULL REFERENCES verifier_run(id) ON DELETE CASCADE,
    sample_a       TEXT NOT NULL,
    group_a        TEXT NOT NULL,
    sample_b       TEXT NOT NULL,
    group_b        TEXT NOT NULL,
    component_size INTEGER NOT NULL,
    -- One real connecting path between the two samples, ordered endpoint to
    -- endpoint, stored as a JSON array of sample numbers.
    path_json      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_conflict_run ON verifier_conflict(run_id);

-- Disk-backed working tables for the streaming verifier. Keeping the
-- adjacency, BFS frontier and parent pointers in SQLite means the Python
-- process never holds the full graph or all search state in memory.
CREATE TABLE IF NOT EXISTS verifier_edge (
    run_id INTEGER NOT NULL,
    u      TEXT NOT NULL,
    v      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ve_run_u ON verifier_edge(run_id, u);

CREATE TABLE IF NOT EXISTS verifier_vertex (
    run_id       INTEGER NOT NULL,
    sample_no    TEXT NOT NULL,
    grp          TEXT,
    component_id INTEGER,
    visited      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_vv_run_visited
    ON verifier_vertex(run_id, visited);
CREATE UNIQUE INDEX IF NOT EXISTS idx_vv_run_sample
    ON verifier_vertex(run_id, sample_no);

CREATE TABLE IF NOT EXISTS verifier_frontier (
    run_id    INTEGER NOT NULL,
    sample_no TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_vf_run ON verifier_frontier(run_id);

CREATE TABLE IF NOT EXISTS verifier_parent (
    run_id INTEGER NOT NULL,
    child  TEXT NOT NULL,
    parent TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_vp_run_child
    ON verifier_parent(run_id, child);
"""


def init_databases() -> None:
    for ddl, path in (
        (BUSINESS_SCHEMA, settings.business_db),
        (CHECK_SCHEMA, settings.check_db),
    ):
        with transaction(path) as conn:
            conn.executescript(ddl)
