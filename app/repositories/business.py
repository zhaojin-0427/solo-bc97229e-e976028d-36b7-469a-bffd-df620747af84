"""Business database repository.

All mutating methods open one short write transaction per request and bump
the data-version ledger exactly once per committed change. The background
verifier never writes here, so verification work cannot hold the business
database write lock.
"""
from __future__ import annotations

import csv
import io
import os
import sqlite3
from typing import List, Optional, Tuple

from ..config import settings
from ..database import connect, transaction


class BusinessRepository:
    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or settings.business_db

    # --- read helpers -----------------------------------------------------

    def current_version(self) -> int:
        with connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS v FROM data_version"
            ).fetchone()
            return int(row["v"])

    def counts(self) -> Tuple[int, int]:
        with connect(self.db_path) as conn:
            edges = conn.execute(
                "SELECT COUNT(*) c FROM derivation_edge"
            ).fetchone()["c"]
            batch = conn.execute(
                "SELECT batch_id FROM group_batch ORDER BY batch_id DESC LIMIT 1"
            ).fetchone()
            samples = 0
            if batch:
                samples = conn.execute(
                    "SELECT COUNT(*) c FROM sample_group WHERE batch_id=?",
                    (batch["batch_id"],),
                ).fetchone()["c"]
            return int(edges), int(samples)

    def all_edges(self) -> List[Tuple[str, str]]:
        with connect(self.db_path) as conn:
            return [
                (r["parent_sample"], r["child_sample"])
                for r in conn.execute(
                    "SELECT parent_sample, child_sample FROM derivation_edge"
                )
            ]

    def active_groups(self) -> dict[str, str]:
        with connect(self.db_path) as conn:
            batch = conn.execute(
                "SELECT batch_id FROM group_batch ORDER BY batch_id DESC LIMIT 1"
            ).fetchone()
            if not batch:
                return {}
            return {
                r["sample_no"]: r["grp"]
                for r in conn.execute(
                    "SELECT sample_no, grp FROM sample_group WHERE batch_id=?",
                    (batch["batch_id"],),
                )
            }

    def acceptance(self) -> Optional[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM acceptance WHERE status='active' "
                "ORDER BY accepted_version DESC LIMIT 1"
            ).fetchone()

    def artifact(self, version: int) -> Optional[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return conn.execute(
                "SELECT * FROM release_artifact WHERE accepted_version=?",
                (version,),
            ).fetchone()

    # --- mutations --------------------------------------------------------

    def add_relations(self, edges: List[Tuple[str, str]]) -> dict:
        """Append-only edge insert; duplicates on the natural key merge."""
        with transaction(self.db_path) as conn:
            next_version = (
                conn.execute(
                    "SELECT COALESCE(MAX(version),0) + 1 AS v FROM data_version"
                ).fetchone()["v"]
            )
            inserted = 0
            for parent, child in edges:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO derivation_edge"
                    "(parent_sample, child_sample, version) VALUES (?, ?, ?)",
                    (parent, child, next_version),
                )
                inserted += cur.rowcount
            duplicate = len(edges) - inserted
            version = self._bump_version(
                conn,
                "relation",
                f"上传衍生关系 {len(edges)} 条，新增 {inserted} 条，"
                f"重复合一 {duplicate} 条",
            )
            total = conn.execute(
                "SELECT COUNT(*) c FROM derivation_edge"
            ).fetchone()["c"]
            self._invalidate_acceptance(conn)
        return {
            "data_version": version,
            "inserted_rows": inserted,
            "duplicate_rows_merged": duplicate,
            "total_rows_after": total,
        }

    def replace_groups(self, assignments: List[Tuple[str, str]]) -> dict:
        """Whole-file replacement: old rows stay in history but are inactive."""
        with transaction(self.db_path) as conn:
            next_version = (
                conn.execute(
                    "SELECT COALESCE(MAX(version),0) + 1 AS v FROM data_version"
                ).fetchone()["v"]
            )
            cur = conn.execute(
                "INSERT INTO group_batch(version, sample_count) VALUES (?, ?)",
                (next_version, len(assignments)),
            )
            batch_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO sample_group(sample_no, grp, batch_id, version) "
                "VALUES (?, ?, ?, ?)",
                [(s, g, batch_id, next_version) for s, g in assignments],
            )
            version = self._bump_version(
                conn,
                "groups",
                f"整份替换分组 {len(assignments)} 个样本（批次 {batch_id}）",
            )
            self._invalidate_acceptance(conn)
        return {
            "data_version": version,
            "inserted_rows": len(assignments),
            "duplicate_rows_merged": 0,
            "total_rows_after": len(assignments),
        }

    # --- acceptance / artifacts ------------------------------------------

    def accept_version(
        self, version: int, run_id: int, groups: dict[str, str]
    ) -> str:
        """Mark *version* as the single active acceptance and freeze its CSV.

        Returns the artifact file path. Any previously active acceptance is
        superseded in the same transaction.
        """
        os.makedirs(settings.snapshot_dir, exist_ok=True)
        file_path = os.path.join(
            settings.snapshot_dir, f"groups_v{version}_passed.csv"
        )
        # Build the frozen file first, outside the DB transaction; a failed
        # write leaves the database untouched.
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["样本号", "组别"])
        for sample in sorted(groups):
            writer.writerow([sample, groups[sample]])
        with open(file_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(buffer.getvalue())

        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE acceptance SET status='superseded' WHERE status='active'"
            )
            conn.execute(
                "INSERT INTO acceptance(accepted_version, run_id) VALUES (?, ?)",
                (version, run_id),
            )
            conn.execute(
                "INSERT OR REPLACE INTO release_artifact"
                "(accepted_version, file_path, row_count) VALUES (?, ?, ?)",
                (version, file_path, len(groups)),
            )
        return file_path

    def list_versions(self, limit: int = 50) -> List[sqlite3.Row]:
        with connect(self.db_path) as conn:
            return list(
                conn.execute(
                    "SELECT * FROM data_version ORDER BY version DESC LIMIT ?",
                    (limit,),
                )
            )

    # --- internals --------------------------------------------------------

    @staticmethod
    def _bump_version(
        conn: sqlite3.Connection, change_type: str, detail: str
    ) -> int:
        cur = conn.execute(
            "INSERT INTO data_version(change_type, detail) VALUES (?, ?)",
            (change_type, detail),
        )
        return int(cur.lastrowid)

    @staticmethod
    def _invalidate_acceptance(conn: sqlite3.Connection) -> None:
        """Every committed change revokes the previous release."""
        conn.execute(
            "UPDATE acceptance SET status='superseded' WHERE status='active'"
        )
