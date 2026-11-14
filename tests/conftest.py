"""Pytest fixtures: isolated temp data dir + TestClient with fast polling.

Each test gets a fresh data directory, so database state never leaks between
tests. The settings object is module-level, so we point it at the temp dir
and re-initialise the databases per test.
"""
from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

# Point data at a writable temp dir before the app package is imported.
_TMP_ROOT = tempfile.mkdtemp(prefix="leakcheck-root-")
os.environ["LEAKCHECK_DATA_DIR"] = _TMP_ROOT
# Fast worker polling / stale-run timeout for deterministic tests.
os.environ.setdefault("LEAKCHECK_POLL_INTERVAL", "0.05")
os.environ.setdefault("LEAKCHECK_HEARTBEAT_TIMEOUT", "5")
# Most tests drive the verifier deterministically; the worker-specific test
# re-enables the thread explicitly.
os.environ["LEAKCHECK_DISABLE_WORKER"] = "1"

from app import config as config_module  # noqa: E402
from app.database import init_databases  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services.worker import VerificationWorker  # noqa: E402


@pytest.fixture()
def data_dir(monkeypatch):
    tmp = tempfile.mkdtemp(prefix="leakcheck-test-")
    monkeypatch.setattr(config_module.settings, "data_dir", tmp, raising=False)
    monkeypatch.setattr(
        config_module.settings,
        "business_db",
        os.path.join(tmp, "business.db"),
        raising=False,
    )
    monkeypatch.setattr(
        config_module.settings,
        "check_db",
        os.path.join(tmp, "check.db"),
        raising=False,
    )
    monkeypatch.setattr(
        config_module.settings,
        "snapshot_dir",
        os.path.join(tmp, "snapshots"),
        raising=False,
    )
    init_databases()
    return tmp


@pytest.fixture()
def app(data_dir):
    application = create_app()
    return application


@pytest.fixture()
def client(app):
    with TestClient(app) as c:
        yield c
    app.state.worker.stop()


@pytest.fixture()
def worker():
    w = VerificationWorker()
    yield w
    w.stop()


def csv_bytes(header: str, rows: list[tuple[str, str]]) -> bytes:
    lines = [header] + [",".join(r) for r in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


@pytest.fixture()
def make_csv():
    return csv_bytes
