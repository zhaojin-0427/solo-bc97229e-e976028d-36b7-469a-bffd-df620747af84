"""Verifier-side repository (check.db).

Runs are the unit of work. The API enqueues a run bound to a specific data
version; exactly one worker across both processes claims each queued/stale
run via an atomic ``UPDATE ... WHERE status`` claim. A late-finishing run can
never overwrite newer state: conclusions are only written while the run is
still the active claimant, and the business side validates the version
match before accepting anything.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from ..config import settings
from ..database import connect, transaction


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CheckRepository:
    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or settings.check_db

    # --- queueing ---------------------------------------------------------

    def enqueue(self, data_version: int) -> int:
        """Queue a verification for *data_version*.

        Identical queued/running runs are coalesced so repeated uploads do
        not pile up duplicate work; a finished run for the same version is
        never reused because the data content at a version is immutable.
        """
        with transaction(self.db_path) as conn:
            existing = conn.execute(
                "SELECT id FROM verifier_run WHERE data_version=? "
                "AND status IN ('queued','running')",
                (data_version,),
            ).fetchone()
            if existing:
                return int(existing["id"])
            # Newer versions supersede unfinished older runs.
            conn.execute(
                "UPDATE verifier_run SET status='superseded', finished_at=? "
                "WHERE status IN ('queued','running') AND data_version < ?",
                (_utcnow().isoformat(), data_version),
            )
            cur = conn.execute(
                "INSERT INTO verifier_run(data_version, status) VALUES (?, 'queued')",
                (data_version,),
            )
            return int(cur.lastrowid)

    def claim_next_run(self, worker_id: str) -> Optional[sqlite3.Row]:
        """Atomically claim the highest-priority (newest) queued run.

        Also recovers runs whose worker died (heartbeat stale) after the
        attempt limit is not yet reached.
        """
        with transaction(self.db_path) as conn:
            # First reap dead workers.
            stale_cutoff = (
                _utcnow() - timedelta(seconds=settings.heartbeat_timeout)
            ).isoformat()
            stale = conn.execute(
                "SELECT id, attempts FROM verifier_run "
                "WHERE status='running' AND heartbeat_at IS NOT NULL "
                "AND heartbeat_at < ? "
                "AND data_version = (SELECT COALESCE(MAX(data_version),0) "
                "                     FROM verifier_run)",
                (stale_cutoff,),
            ).fetchall()
            for row in stale:
                if row["attempts"] >= settings.max_attempts:
                    conn.execute(
                        "UPDATE verifier_run SET status='failed', finished_at=?, "
                        "error_message=? WHERE id=?",
                        (
                            _utcnow().isoformat(),
                            f"连续 {row['attempts']} 次核查心跳超时，判定失败",
                            row["id"],
                        ),
                    )
                else:
                    # Re-queue but keep the accumulated attempt count, so the
                    # max_attempts ceiling still applies across worker deaths.
                    conn.execute(
                        "UPDATE verifier_run SET status='queued', "
                        "progress_pct=0, vertices_processed=0, worker_id=NULL, "
                        "heartbeat_at=NULL WHERE id=?",
                        (row["id"],),
                    )
            run = conn.execute(
                "SELECT * FROM verifier_run WHERE status='queued' "
                "ORDER BY data_version DESC LIMIT 1"
            ).fetchone()
            if not run:
                return None
            conn.execute(
                "UPDATE verifier_run SET status='running', started_at=?, "
                "worker_id=?, heartbeat_at=?, attempts=attempts+1 WHERE id=?",
                (
                    _utcnow().isoformat(),
                    worker_id,
                    _utcnow().isoformat(),
                    run["id"],
                ),
            )
            return conn.execute(
                "SELECT * FROM verifier_run WHERE id=?", (run["id"],)
            ).fetchone()

    def heartbeat(self, run_id: int, processed: int, total: int) -> None:
        pct = round(100.0 * processed / total, 2) if total else 0.0
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE verifier_run SET heartbeat_at=?, vertices_processed=?, "
                "vertices_total=?, progress_pct=? WHERE id=? AND status='running'",
                (_utcnow().isoformat(), processed, total, pct, run_id),
            )

    def finish_run(
        self,
        run_id: int,
        status: str,
        components: int,
        conflicts: List[dict],
        error: Optional[str] = None,
    ) -> bool:
        """Persist the conclusion. Returns False if the run was superseded.

        A superseded run's result is discarded: late verification must not
        overwrite the state of a newer data version.
        """
        with transaction(self.db_path) as conn:
            run = conn.execute(
                "SELECT status FROM verifier_run WHERE id=?", (run_id,)
            ).fetchone()
            if not run or run["status"] != "running":
                return False
            conn.execute(
                "UPDATE verifier_run SET status=?, finished_at=?, "
                "progress_pct=100, vertices_processed=COALESCE(vertices_total, "
                "vertices_processed), components_total=?, conflicts_total=?, "
                "error_message=? WHERE id=?",
                (
                    status,
                    _utcnow().isoformat(),
                    components,
                    len(conflicts),
                    error,
                    run_id,
                ),
            )
            conn.execute("DELETE FROM verifier_conflict WHERE run_id=?", (run_id,))
            conn.executemany(
                "INSERT INTO verifier_conflict(run_id, sample_a, group_a, "
                "sample_b, group_b, component_size, path_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        run_id,
                        c["sample_a"],
                        c["group_a"],
                        c["sample_b"],
                        c["group_b"],
                        c["component_size"],
                        json.dumps(c["path"], ensure_ascii=False),
                    )
                    for c in conflicts[: settings.max_conflicts]
                ],
            )
        return True

    # --- queries ----------------------------------------------------------

    def get_run(self, run_id: int) -> Optional[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM verifier_run WHERE id=?", (run_id,)
            ).fetchone()

    def latest_run(self) -> Optional[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM verifier_run ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def latest_run_for_version(self, version: int) -> Optional[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM verifier_run WHERE data_version=? "
                "ORDER BY id DESC LIMIT 1",
                (version,),
            ).fetchone()

    def conflicts_for(self, run_id: int) -> List[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return list(
                conn.execute(
                    "SELECT * FROM verifier_conflict WHERE run_id=? ORDER BY id",
                    (run_id,),
                )
            )

    def running_versions(self) -> List[int]:
        with connect(self.db_path) as conn:
            return [
                int(r["data_version"])
                for r in conn.execute(
                    "SELECT DISTINCT data_version FROM verifier_run "
                    "WHERE status IN ('queued','running')"
                )
            ]
