"""Streaming cross-group leakage verifier.

The derivation relation is treated as an *undirected* graph. Two samples are
in conflict when they are connected by any path and carry different groups
(samples without a group assignment are neutral and never trigger conflicts).

Memory discipline (requirement: never hold the whole graph or all search
state in memory):

* the immutable snapshot at the bound data version is copied edge by edge
  into disk-backed working tables in ``check.db``;
* BFS frontier and parent pointers live in SQLite tables, not Python sets;
* Python only ever holds one edge/vertex batch and one reconstructed path.

The business database is opened read-only through a snapshot transaction, so
uploads and queries remain available during verification and no business-DB
write transaction is held.
"""
from __future__ import annotations

import logging
import sqlite3
import uuid
from typing import Dict, List, Optional, Tuple

from ..config import settings
from ..database import connect
from ..repositories.business import BusinessRepository
from ..repositories.check import CheckRepository

logger = logging.getLogger("leakcheck.verifier")

BATCH = 2_000


def build_worker_id() -> str:
    import socket

    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


class GraphVerifier:
    def __init__(
        self,
        check_repo: Optional[CheckRepository] = None,
        business_repo: Optional[BusinessRepository] = None,
    ) -> None:
        self.check = check_repo or CheckRepository()
        self.business = business_repo or BusinessRepository()

    # --- snapshot loading -------------------------------------------------

    def _load_snapshot(
        self, work: sqlite3.Connection, run_id: int, version: int
    ) -> Tuple[int, int]:
        """Materialise the graph exactly as it existed at *version*.

        Edges are append-only, so filtering by ``version <= ?`` reconstructs
        the historical edge set. Groups use the newest batch whose own
        version is <= the bound version.
        """
        work.execute("DELETE FROM verifier_edge WHERE run_id=?", (run_id,))
        work.execute("DELETE FROM verifier_vertex WHERE run_id=?", (run_id,))

        read = self._open_ro()
        try:
            current = read.execute(
                "SELECT COALESCE(MAX(version),0) v FROM data_version"
            ).fetchone()["v"]
            if current < version:
                raise RuntimeError(
                    f"核查目标版本 {version} 高于当前版本 {current}，数据已回退？"
                )
            # Undirected: store both directions for every edge.
            edge_count = 0
            batch: List[tuple] = []
            rows = read.execute(
                "SELECT parent_sample, child_sample FROM derivation_edge "
                "WHERE version <= ?",
                (version,),
            )
            for parent, child in rows:
                batch.extend(
                    [(run_id, parent, child), (run_id, child, parent)]
                )
                edge_count += 1
                if len(batch) >= BATCH:
                    work.executemany(
                        "INSERT INTO verifier_edge(run_id,u,v) VALUES (?,?,?)",
                        batch,
                    )
                    batch.clear()
            if batch:
                work.executemany(
                    "INSERT INTO verifier_edge(run_id,u,v) VALUES (?,?,?)", batch
                )

            batch_row = read.execute(
                "SELECT batch_id FROM group_batch WHERE version <= ? "
                "ORDER BY version DESC, batch_id DESC LIMIT 1",
                (version,),
            ).fetchone()
            group_rows = []
            if batch_row:
                group_rows = read.execute(
                    "SELECT sample_no, grp FROM sample_group WHERE batch_id=?",
                    (batch_row["batch_id"],),
                ).fetchall()
        finally:
            read.close()

        groups: Dict[str, str] = {r["sample_no"]: r["grp"] for r in group_rows}
        # Vertices = union of edge endpoints and grouped samples.
        vertices = set(groups)
        for r in work.execute(
            "SELECT DISTINCT u FROM verifier_edge WHERE run_id=?", (run_id,)
        ):
            vertices.add(r["u"])
        vert_batch = [
            (run_id, sample, groups.get(sample)) for sample in vertices
        ]
        for i in range(0, len(vert_batch), BATCH):
            work.executemany(
                "INSERT INTO verifier_vertex(run_id, sample_no, grp) "
                "VALUES (?,?,?)",
                vert_batch[i : i + BATCH],
            )
        work.commit()
        return len(vertices), edge_count

    @staticmethod
    def _open_ro() -> sqlite3.Connection:
        """Open the business DB read-only; writes are impossible by design."""
        from ..database import _apply_pragmas  # reuse row factory + busy timeout

        uri = f"file:{settings.business_db}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15)
        _apply_pragmas(conn)
        return conn

    # --- BFS --------------------------------------------------------------

    def _component_groups(
        self, work: sqlite3.Connection, run_id: int, component_id: int
    ) -> Dict[str, str]:
        """Return one representative sample per group inside a component."""
        reps: Dict[str, str] = {}
        for r in work.execute(
            "SELECT sample_no, grp FROM verifier_vertex "
            "WHERE run_id=? AND component_id=? AND grp IS NOT NULL",
            (run_id, component_id),
        ):
            reps.setdefault(r["grp"], r["sample_no"])
        return reps

    def _ancestor_chain(
        self, work: sqlite3.Connection, run_id: int, node: str
    ) -> List[str]:
        """Return [node, parent, grandparent, ...] until the chain ends."""
        chain: List[str] = []
        current = node
        for _ in range(1_000_000):
            chain.append(current)
            row = work.execute(
                "SELECT parent FROM verifier_parent WHERE run_id=? AND child=?",
                (run_id, current),
            ).fetchone()
            if row is None:  # component root has no parent row
                break
            current = row["parent"]
        return chain

    def _path_between(
        self,
        work: sqlite3.Connection,
        run_id: int,
        anchor: str,
        target: str,
    ) -> List[str]:
        """Return one real anchor->target path via the BFS parent tree.

        We take the full ancestor set of ``anchor`` (all nodes reachable by
        following parent pointers, including ``anchor`` itself). Then we walk
        ``target``'s parent chain until the first node in that set: that node
        is the junction of the two tree branches. The final path is

            anchor -> ... -> junction -> ... -> target

        which is a genuine graph path regardless of how the parent pointers
        are oriented relative to the BFS root.
        """
        anchor_chain = self._ancestor_chain(work, run_id, anchor)
        anchor_set = set(anchor_chain)
        target_chain = self._ancestor_chain(work, run_id, target)

        junction_pos = next(
            pos for pos, node in enumerate(target_chain) if node in anchor_set
        )
        junction = target_chain[junction_pos]
        anchor_pos = anchor_chain.index(junction)

        anchor_to_junction = anchor_chain[: anchor_pos + 1]
        junction_to_target = list(reversed(target_chain[:junction_pos]))
        return anchor_to_junction + junction_to_target

    def _bfs_component(
        self,
        work: sqlite3.Connection,
        run_id: int,
        root: str,
        component_id: int,
        heartbeat_cb,
        processed: int,
        total: int,
    ) -> Tuple[int, List[dict], int]:
        work.execute("DELETE FROM verifier_frontier WHERE run_id=?", (run_id,))
        work.execute("DELETE FROM verifier_parent WHERE run_id=?", (run_id,))
        work.execute(
            "INSERT INTO verifier_frontier(run_id, sample_no) VALUES (?,?)",
            (run_id, root),
        )
        work.execute(
            "UPDATE verifier_vertex SET visited=1, component_id=? "
            "WHERE run_id=? AND sample_no=?",
            (component_id, run_id, root),
        )
        size = 0
        while True:
            nodes = [
                r["sample_no"]
                for r in work.execute(
                    "SELECT sample_no FROM verifier_frontier WHERE run_id=?"
                    " LIMIT ?",
                    (run_id, BATCH),
                )
            ]
            if not nodes:
                break
            placeholders = ",".join("?" for _ in nodes)
            work.execute(
                f"DELETE FROM verifier_frontier WHERE run_id=? "
                f"AND sample_no IN ({placeholders})",
                (run_id, *nodes),
            )
            next_frontier: List[tuple] = []
            for node in nodes:
                size += 1
                processed += 1
                for r in work.execute(
                    "SELECT v FROM verifier_edge WHERE run_id=? AND u=?",
                    (run_id, node),
                ):
                    neighbour = r["v"]
                    owner = work.execute(
                        "SELECT visited FROM verifier_vertex "
                        "WHERE run_id=? AND sample_no=?",
                        (run_id, neighbour),
                    ).fetchone()
                    if owner["visited"]:
                        continue
                    work.execute(
                        "UPDATE verifier_vertex SET visited=1, component_id=? "
                        "WHERE run_id=? AND sample_no=?",
                        (component_id, run_id, neighbour),
                    )
                    work.execute(
                        "INSERT INTO verifier_parent(run_id,child,parent) "
                        "VALUES (?,?,?)",
                        (run_id, neighbour, node),
                    )
                    next_frontier.append((run_id, neighbour))
                if processed % BATCH == 0:
                    work.commit()
                    heartbeat_cb(processed, total)
            if next_frontier:
                work.executemany(
                    "INSERT INTO verifier_frontier(run_id,sample_no) VALUES (?,?)",
                    next_frontier,
                )
            work.commit()
            heartbeat_cb(processed, total)

        conflicts: List[dict] = []
        groups = self._component_groups(work, run_id, component_id)
        if len(groups) > 1:
            # Use the first group's representative as a stable anchor and
            # report one conflict per other group. Representatives come from
            # the component group map, so this does not depend on where the
            # BFS root happens to sit in the parent tree.
            group_names = sorted(groups)
            anchor_group = group_names[0]
            anchor_sample = groups[anchor_group]
            for grp in group_names[1:]:
                target = groups[grp]
                full_path = self._path_between(
                    work, run_id, anchor_sample, target
                )
                conflicts.append(
                    {
                        "sample_a": anchor_sample,
                        "group_a": anchor_group,
                        "sample_b": target,
                        "group_b": grp,
                        "component_size": size,
                        "path": full_path,
                    }
                )
        return size, conflicts, processed

    # --- entry point ------------------------------------------------------

    def run(self, run_row: sqlite3.Row) -> None:
        run_id = int(run_row["id"])
        version = int(run_row["data_version"])
        logger.info("run %s starts for data version %s", run_id, version)
        work = connect(settings.check_db)
        try:
            total, edge_count = self._load_snapshot(work, run_id, version)
            work.execute(
                "UPDATE verifier_run SET vertices_total=?, edges_total=? WHERE id=?",
                (total, edge_count, run_id),
            )
            work.commit()

            def heartbeat(processed: int, tot: int) -> None:
                self.check.heartbeat(run_id, processed, tot)

            component_id = 0
            all_conflicts: List[dict] = []
            processed = 0
            while True:
                # Prefer a grouped sample as the BFS root: this keeps every
                # reported group representative inside one rooted parent tree
                # whose root is itself one of the conflict endpoints.
                row = work.execute(
                    "SELECT sample_no FROM verifier_vertex "
                    "WHERE run_id=? AND visited=0 AND grp IS NOT NULL "
                    "LIMIT 1",
                    (run_id,),
                ).fetchone()
                if row is None:
                    row = work.execute(
                        "SELECT sample_no FROM verifier_vertex "
                        "WHERE run_id=? AND visited=0 LIMIT 1",
                        (run_id,),
                    ).fetchone()
                if row is None:
                    break
                component_id += 1
                _, conflicts, processed = self._bfs_component(
                    work,
                    run_id,
                    row["sample_no"],
                    component_id,
                    heartbeat,
                    processed,
                    total,
                )
                all_conflicts.extend(conflicts)
                heartbeat(processed, total)

            status = "passed" if not all_conflicts else "failed"
            accepted = self.check.finish_run(
                run_id, status, component_id, all_conflicts
            )
            if not accepted:
                logger.warning(
                    "run %s superseded before finish; result discarded", run_id
                )
                return
            if status == "passed":
                self._promote_if_current(version, run_id)
            logger.info(
                "run %s finished: %s (%s conflicts, %s components)",
                run_id,
                status,
                len(all_conflicts),
                component_id,
            )
        except Exception as exc:  # noqa: BLE001 - record and retry later
            logger.exception("run %s errored", run_id)
            self.check.finish_run(
                run_id, "failed", 0, [], error=f"{type(exc).__name__}: {exc}"
            )
        finally:
            # Reclaim disk working space for this run.
            for table in (
                "verifier_edge",
                "verifier_vertex",
                "verifier_frontier",
                "verifier_parent",
            ):
                work.execute(f"DELETE FROM {table} WHERE run_id=?", (run_id,))
            work.commit()
            work.close()

    def _promote_if_current(self, version: int, run_id: int) -> None:
        """Only accept when the conclusion still matches the latest version."""
        current = self.business.current_version()
        if current != version:
            logger.info(
                "run %s passed v%s but current is v%s; not releasing",
                run_id,
                version,
                current,
            )
            return
        groups = self.business.active_groups()
        self.business.accept_version(version, run_id, groups)
