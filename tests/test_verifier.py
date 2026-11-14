"""Verifier-level tests: streaming BFS correctness and two-process sharing.

These exercise the disk-backed graph algorithm directly on larger inputs,
proving that correctness does not depend on holding the graph in memory.
"""
from __future__ import annotations

import threading

from app.repositories.business import BusinessRepository
from app.repositories.check import CheckRepository
from app.services.verifier import GraphVerifier
from app.services.worker import VerificationWorker


def _seed_chain_with_bridge():
    """Two chains joined by a bridge; groups conflict across the bridge."""
    business = BusinessRepository()
    edges = []
    # chain 1: c1-0 ... c1-49 ; chain 2: c2-0 ... c2-49
    for i in range(49):
        edges.append((f"c1-{i}", f"c1-{i+1}"))
        edges.append((f"c2-{i}", f"c2-{i+1}"))
    edges.append(("c1-49", "c2-0"))  # bridge -> one big component
    business.add_relations(edges)
    groups = [("c1-0", "train"), ("c2-49", "val")]
    business.replace_groups(groups)
    return len(edges), edges


def test_large_graph_disk_bfs_finds_conflict_and_path(data_dir):
    edge_count, edges = _seed_chain_with_bridge()
    version = BusinessRepository().current_version()
    check = CheckRepository()
    run_id = check.enqueue(version)
    run = check.claim_next_run("unit-worker")
    GraphVerifier(check, BusinessRepository()).run(run)

    finished = check.get_run(run_id)
    assert finished["status"] == "failed"
    assert finished["edges_total"] == edge_count
    assert finished["vertices_total"] == 100
    conflicts = check.conflicts_for(run_id)
    assert len(conflicts) == 1
    c = conflicts[0]
    assert {c["sample_a"], c["sample_b"]} == {"c1-0", "c2-49"}
    assert {c["group_a"], c["group_b"]} == {"train", "val"}
    import json

    path = json.loads(c["path_json"])
    # The requirement is one REAL connecting path between the conflicting
    # samples (BFS-tree path); it must be edge-valid and endpoint-correct.
    assert set([path[0], path[-1]]) == {"c1-0", "c2-49"}
    assert "c1-49" in path and "c2-0" in path
    edgeset = {tuple(sorted(e)) for e in edges}
    for a, b_ in zip(path, path[1:]):
        assert tuple(sorted((a, b_))) in edgeset
    # no repeated vertex -> it is a simple path
    assert len(path) == len(set(path))


def test_large_graph_clean_when_groups_consistent(data_dir):
    _seed_chain_with_bridge()
    # Replace groups so the whole component shares one group.
    BusinessRepository().replace_groups(
        [("c1-0", "train"), ("c2-49", "train")]
    )
    version = BusinessRepository().current_version()
    check = CheckRepository()
    run_id = check.enqueue(version)
    run = check.claim_next_run("unit-worker")
    GraphVerifier(check, BusinessRepository()).run(run)
    finished = check.get_run(run_id)
    assert finished["status"] == "passed"
    assert finished["components_total"] == 1
    assert finished["conflicts_total"] == 0
    assert BusinessRepository().acceptance() is not None


def test_ungrouped_samples_are_neutral(data_dir):
    BusinessRepository().add_relations([("A", "B"), ("B", "C")])
    BusinessRepository().replace_groups([("A", "train"), ("C", "train")])
    check = CheckRepository()
    rid = check.enqueue(BusinessRepository().current_version())
    GraphVerifier(check, BusinessRepository()).run(check.claim_next_run("w"))
    assert check.get_run(rid)["status"] == "passed"


def test_two_workers_claim_distinct_runs(data_dir):
    """Simulate two processes: only one claimant wins each run."""
    BusinessRepository().replace_groups([("A", "train")])
    check = CheckRepository()
    rid = check.enqueue(1)
    won = []

    def claim(worker_name):
        row = check.claim_next_run(worker_name)
        if row is not None:
            won.append((worker_name, row["id"]))

    t1 = threading.Thread(target=claim, args=("proc-1",))
    t2 = threading.Thread(target=claim, args=("proc-2",))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert len(won) == 1
    assert won[0][1] == rid
    # Second claimant gets nothing while the run is in progress.
    assert check.claim_next_run("proc-3") is None


def test_stale_heartbeat_run_is_reclaimed(data_dir):
    BusinessRepository().replace_groups([("A", "train")])
    check = CheckRepository()
    rid = check.enqueue(1)
    first = check.claim_next_run("dead-worker")
    assert first["attempts"] == 1
    # Force the heartbeat far into the past using the same ISO-UTC format.
    import sqlite3
    from datetime import datetime, timedelta, timezone

    from app.config import settings

    stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    with sqlite3.connect(settings.check_db) as conn:
        conn.execute(
            "UPDATE verifier_run SET heartbeat_at=? WHERE id=?",
            (stale, rid),
        )
        conn.commit()
    reclaimed = check.claim_next_run("live-worker")
    assert reclaimed["id"] == rid
    assert reclaimed["worker_id"] == "live-worker"
    assert reclaimed["attempts"] == 2  # original claim (1) plus retry (2)


def test_worker_restart_rechecks_latest_and_does_not_restore_old_release(data_dir):
    """Simulate a crash between an upload and its verification.

    The upload bumps the version and revokes the old acceptance, but here no
    run exists for the new version yet (as if the process died right after
    commit). On restart, reconcile must enqueue a fresh run for the latest
    version without restoring the old release.
    """
    business = BusinessRepository()
    check = CheckRepository()
    business.replace_groups([("A", "train")])
    check.enqueue(1)
    GraphVerifier(check, business).run(check.claim_next_run("w"))
    assert business.acceptance() is not None

    # New change revokes acceptance; deliberately do NOT enqueue a run,
    # reproducing a crash immediately after the business commit.
    business.replace_groups([("A", "val")])
    assert business.acceptance() is None
    assert check.latest_run_for_version(2) is None

    # Boot-time reconciliation picks up the unverified latest version.
    VerificationWorker().reconcile()
    latest = check.latest_run()
    assert latest["data_version"] == 2
    assert latest["status"] == "queued"


def test_reconcile_does_not_loop_on_failed_version(data_dir):
    """A failed run is terminal for that version; reconcile must not re-queue."""
    business = BusinessRepository()
    check = CheckRepository()
    business.add_relations([("A", "B")])  # connected, so group split conflicts
    business.replace_groups([("A", "train"), ("B", "val")])
    rid = check.enqueue(business.current_version())
    GraphVerifier(check, business).run(check.claim_next_run("w"))
    assert check.get_run(rid)["status"] == "failed"

    runs_before = check.latest_run()["id"]
    worker = VerificationWorker()
    for _ in range(3):
        worker.reconcile()
    assert check.latest_run()["id"] == runs_before  # no new runs created
    assert business.acceptance() is None  # failure never grants a release
