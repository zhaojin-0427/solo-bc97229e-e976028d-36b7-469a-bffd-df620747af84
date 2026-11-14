"""End-to-end behaviour tests driven through the FastAPI TestClient.

The verifier is exercised directly (same code path the worker thread runs)
so assertions stay deterministic; one test also runs the real worker loop.
"""
from __future__ import annotations

import time

from app.repositories.business import BusinessRepository
from app.repositories.check import CheckRepository
from app.services.verifier import GraphVerifier

from .conftest import csv_bytes


REL_CSV = [
    ("S001", "D001"),
    ("S002", "D002"),
    ("D001", "E001"),  # chain S001-D001-E001
    ("S003", "D003"),
]
GROUP_CSV_OK = [
    ("S001", "train"),
    ("D001", "train"),
    ("E001", "train"),
    ("S002", "val"),
    ("D002", "val"),
    ("S003", "test"),
    ("D003", "test"),
]


def _run_verification_now(expected_version: int) -> dict:
    check = CheckRepository()
    business = BusinessRepository()
    worker_check = check
    run_row = None
    # claim loop mirrors the worker scheduling logic
    for _ in range(50):
        run_row = check.claim_next_run("test-worker")
        if run_row:
            break
        time.sleep(0.05)
    assert run_row is not None, "没有可领取的核查任务"
    assert run_row["data_version"] == expected_version
    GraphVerifier(worker_check, business).run(run_row)
    return check.get_run(run_row["id"])


# --- happy path ------------------------------------------------------------

def test_full_flow_pass_and_frozen_download(client, make_csv):
    # relations
    r = client.post(
        "/api/upload/relations",
        files={"file": ("rel.csv", make_csv("原样本号,衍生样本号", REL_CSV), "text/csv")},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["data_version"] == 1
    assert body["inserted_rows"] == 4
    assert body["verification"]["status"] in ("queued", "running")

    # groups (whole replacement) -> version 2
    r = client.post(
        "/api/upload/groups",
        files={"file": ("grp.csv", make_csv("样本号,组别", GROUP_CSV_OK), "text/csv")},
    )
    assert r.status_code == 200
    assert r.json()["data_version"] == 2

    run = _run_verification_now(2)
    assert run["status"] == "passed"
    assert run["components_total"] >= 3

    status = client.get("/api/status").json()
    assert status["download_available"] is True
    assert status["accepted_version"] == 2

    dl = client.get("/api/release/groups.csv")
    assert dl.status_code == 200
    assert dl.headers["x-release-version"] == "2"
    content = dl.content.decode("utf-8")
    assert "样本号,组别" in content
    assert "S001,train" in content


def test_duplicate_relations_merge(client, make_csv):
    # Rows duplicated inside one file are collapsed during parsing.
    payload = make_csv("原样本号,衍生样本号", [("A", "B"), ("A", "B"), ("B", "C")])
    r = client.post(
        "/api/upload/relations", files={"file": ("r.csv", payload, "text/csv")}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["inserted_rows"] == 2
    assert body["total_rows_after"] == 2

    # Re-uploading the same file: every edge already exists and merges away.
    r = client.post(
        "/api/upload/relations", files={"file": ("r.csv", payload, "text/csv")}
    )
    body = r.json()
    assert body["inserted_rows"] == 0
    assert body["duplicate_rows_merged"] == 2
    assert body["total_rows_after"] == 2

    # Partial overlap: one new edge, one merged.
    r = client.post(
        "/api/upload/relations",
        files={
            "file": (
                "r3.csv",
                make_csv("原样本号,衍生样本号", [("A", "B"), ("C", "D")]),
                "text/csv",
            )
        },
    )
    body = r.json()
    assert body["inserted_rows"] == 1
    assert body["duplicate_rows_merged"] == 1
    assert body["total_rows_after"] == 3


def test_group_whole_replacement(client, make_csv):
    client.post(
        "/api/upload/groups",
        files={"file": ("g1.csv", make_csv("样本号,组别", [("A", "train"), ("B", "val")]),
                        "text/csv")},
    )
    client.post(
        "/api/upload/groups",
        files={"file": ("g2.csv", make_csv("样本号,组别", [("A", "test")]), "text/csv")},
    )
    groups = BusinessRepository().active_groups()
    assert groups == {"A": "test"}  # old batch fully replaced
    assert BusinessRepository().counts()[1] == 1


def test_same_sample_two_groups_rejected(client, make_csv):
    r = client.post(
        "/api/upload/groups",
        files={"file": ("bad.csv", make_csv("样本号,组别", [("A", "train"), ("A", "val")]),
                        "text/csv")},
    )
    assert r.status_code == 422
    assert "多个组" in r.json()["detail"]
    # nothing landed: still version 0
    assert BusinessRepository().current_version() == 0


def test_bad_csv_rolls_back_entire_batch(client, make_csv):
    # malformed row width inside the file
    raw = "原样本号,衍生样本号\nA,B\nC,D,E\n".encode("utf-8")
    r = client.post(
        "/api/upload/relations", files={"file": ("bad.csv", raw, "text/csv")}
    )
    assert r.status_code == 422
    assert BusinessRepository().counts()[0] == 0


# --- leakage detection -----------------------------------------------------

def test_cross_group_conflict_returns_real_path(client, make_csv):
    client.post(
        "/api/upload/relations",
        files={"file": ("r.csv", make_csv("原样本号,衍生样本号", REL_CSV), "text/csv")},
    )
    leak_groups = [
        ("S001", "train"),
        ("D001", "train"),
        ("E001", "val"),     # connected to S001 but different group
        ("S002", "val"),
        ("D002", "val"),
        ("S003", "test"),
        ("D003", "test"),
    ]
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", leak_groups), "text/csv")},
    )
    version = r.json()["data_version"]
    run = _run_verification_now(version)
    assert run["status"] == "failed"
    assert run["conflicts_total"] >= 1

    detail = client.get(f"/api/runs/{run['id']}").json()
    conflict = detail["conflicts"][0]
    assert {conflict["group_a"], conflict["group_b"]} == {"train", "val"}
    path = conflict["path"]
    # the returned path is a real connection in the undirected graph
    edges = {tuple(sorted(e)) for e in REL_CSV}
    for a, b in zip(path, path[1:]):
        assert tuple(sorted((a, b))) in edges
    assert conflict["sample_a"] in path and conflict["sample_b"] in path

    # failed verification must not enable download
    assert client.get("/api/status").json()["download_available"] is False
    assert client.get("/api/release/groups.csv").status_code == 409


def test_undirected_edge_detects_reverse_direction(client, make_csv):
    # relation stored child->parent direction must still connect the samples
    rel = [("X1", "Y1"), ("Y1", "Z1")]
    grp = [("X1", "train"), ("Y1", "train"), ("Z1", "test")]
    client.post(
        "/api/upload/relations",
        files={"file": ("r.csv", make_csv("原样本号,衍生样本号", rel), "text/csv")},
    )
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", grp), "text/csv")},
    )
    run = _run_verification_now(r.json()["data_version"])
    assert run["status"] == "failed"


# --- release invalidation & version binding --------------------------------

def test_new_upload_revokes_acceptance_and_blocks_download(client, make_csv):
    client.post(
        "/api/upload/relations",
        files={"file": ("r.csv", make_csv("原样本号,衍生样本号", [("A", "B")]), "text/csv")},
    )
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train"), ("B", "train")]),
                        "text/csv")},
    )
    v = r.json()["data_version"]
    _run_verification_now(v)
    assert client.get("/api/status").json()["acceptance_active"] is True

    # any new commit revokes the old release immediately
    client.post(
        "/api/upload/relations",
        files={"file": ("r2.csv", make_csv("原样本号,衍生样本号", [("B", "C")]), "text/csv")},
    )
    st = client.get("/api/status").json()
    assert st["acceptance_active"] is False
    assert st["download_available"] is False
    assert client.get("/api/release/groups.csv").status_code == 409


def test_late_verification_does_not_release_newer_version(client, make_csv):
    # v1: groups only, passes
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train")]), "text/csv")},
    )
    v1 = r.json()["data_version"]
    run1_row = CheckRepository().claim_next_run("test")
    # Before finishing v1, a new upload creates v2.
    client.post(
        "/api/upload/relations",
        files={"file": ("r.csv", make_csv("原样本号,衍生样本号", [("A", "B")]), "text/csv")},
    )
    v2 = BusinessRepository().current_version()
    assert v2 == v1 + 1
    # Now the late v1 run completes: must not create an acceptance for v2.
    GraphVerifier().run(run1_row)
    st = client.get("/api/status").json()
    assert st["current_data_version"] == v2
    assert st["accepted_version"] is None
    # Running the current version properly releases it.
    run2 = _run_verification_now(v2)
    assert run2["status"] == "passed"
    assert client.get("/api/status").json()["accepted_version"] == v2


def test_superseded_run_result_is_discarded(client, make_csv):
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train"), ("B", "val")]),
                        "text/csv")},
    )
    r.json()["data_version"]
    row1 = CheckRepository().claim_next_run("t")
    client.post(
        "/api/upload/groups",
        files={"file": ("g2.csv", make_csv("样本号,组别", [("A", "train"), ("B", "train")]),
                        "text/csv")},
    )
    GraphVerifier().run(row1)  # late finish
    finished = CheckRepository().get_run(row1["id"])
    assert finished["status"] == "superseded"  # result discarded, not written
    assert client.get("/api/status").json()["accepted_version"] is None


# --- progress & worker ------------------------------------------------------

def test_progress_endpoint_shape(client, make_csv):
    r = client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train")]), "text/csv")},
    )
    run_id = r.json()["verification"]["run_id"]
    _run_verification_now(1)
    got = client.get(f"/api/runs/{run_id}").json()
    assert got["run_id"] == run_id
    assert got["status"] == "passed"
    assert got["progress_pct"] == 100.0


def test_background_worker_picks_up_work(data_dir, make_csv, monkeypatch):
    # Re-enable the real worker thread for this end-to-end test.
    monkeypatch.delenv("LEAKCHECK_DISABLE_WORKER", raising=False)
    from fastapi.testclient import TestClient

    from app.main import create_app

    app = create_app()
    with TestClient(app) as client:
        r = client.post(
            "/api/upload/groups",
            files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train")]), "text/csv")},
        )
        run_id = r.json()["verification"]["run_id"]
        deadline = time.time() + 15
        while time.time() < deadline:
            got = client.get(f"/api/runs/{run_id}").json()
            if got["status"] in ("passed", "failed"):
                break
            time.sleep(0.1)
        got = client.get(f"/api/runs/{run_id}").json()
        assert got["status"] == "passed"
        assert client.get("/api/status").json()["download_available"] is True
    app.state.worker.stop()


def test_restart_reconciles_latest_version_without_old_acceptance(client, make_csv):
    client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", make_csv("样本号,组别", [("A", "train")]), "text/csv")},
    )
    _run_verification_now(1)
    # new upload invalidates acceptance; simulate restart reconciliation
    client.post(
        "/api/upload/groups",
        files={"file": ("g2.csv", make_csv("样本号,组别", [("A", "val")]), "text/csv")},
    )
    from app.services.worker import VerificationWorker

    VerificationWorker().reconcile()
    latest = CheckRepository().latest_run()
    assert latest["data_version"] == 2
    assert latest["status"] in ("queued", "running")
    assert client.get("/api/status").json()["acceptance_active"] is False


def test_download_blocked_while_verification_incomplete(client, make_csv):
    """No download before the current version finishes verifying (no unsafe)."""
    client.post(
        "/api/upload/relations",
        files={"file": ("r.csv", make_csv("原样本号,衍生样本号", [("A", "B")]),
                        "text/csv")},
    )
    # A run is queued but never claimed/finished in this test.
    st = client.get("/api/status").json()
    assert st["latest_run"]["status"] == "queued"
    assert st["download_available"] is False
    assert client.get("/api/release/groups.csv").status_code == 409


def test_health_and_versions(client):
    h = client.get("/health").json()
    assert h["status"] == "ok" and h["worker_id"]
    client.post(
        "/api/upload/groups",
        files={"file": ("g.csv", csv_bytes("样本号,组别", [("A", "train")]), "text/csv")},
    )
    versions = client.get("/api/versions").json()
    assert versions[0]["version"] == 1
    assert versions[0]["change_type"] == "groups"
