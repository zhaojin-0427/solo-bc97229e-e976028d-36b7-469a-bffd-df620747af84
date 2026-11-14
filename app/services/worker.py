"""Background verification worker.

Runs inside each API process as a daemon thread. Cross-process safety comes
from the atomic run claim in ``CheckRepository``: of the two processes only
one can claim a given run. If a process dies, the other process's claim
logic reaps the stale heartbeat and re-verifies the newest state itself.

Startup reconciliation guarantees that after a crash/restart the latest data
version always has a fresh run, no old acceptance is restored, and nothing is
reported safe before a run completes.
"""
from __future__ import annotations

import logging
import threading
import time

from ..config import settings
from ..repositories.business import BusinessRepository
from ..repositories.check import CheckRepository
from .verifier import GraphVerifier, build_worker_id

logger = logging.getLogger("leakcheck.worker")


class VerificationWorker:
    def __init__(self) -> None:
        self.worker_id = build_worker_id()
        self.check = CheckRepository()
        self.business = BusinessRepository()
        self.verifier = GraphVerifier(self.check, self.business)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name=f"verifier-{self.worker_id}", daemon=True
        )
        self._thread.start()
        logger.info("verification worker %s started", self.worker_id)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def reconcile(self) -> None:
        """Ensure the newest data version has at least one run.

        Called at startup and before every scheduling pass. A run is enqueued
        only when the current version has *no* run at all (the post-restart
        case). Terminal states (passed / failed / superseded) and in-flight
        states (queued / running) are left untouched: a failed verification is
        a definitive conclusion for that immutable version, so it must not be
        re-verified in a loop. The stale-heartbeat reclaim path in
        ``claim_next_run`` handles genuinely crashed workers instead.
        """
        current = self.business.current_version()
        if current == 0:
            return
        run = self.check.latest_run_for_version(current)
        if run is None:
            run_id = self.check.enqueue(current)
            logger.info(
                "reconcile: enqueued run %s for current version %s",
                run_id,
                current,
            )

    def _loop(self) -> None:
        # Immediate reconciliation after boot handles restart scenarios.
        try:
            self.reconcile()
        except Exception:  # noqa: BLE001
            logger.exception("startup reconciliation failed")
        while not self._stop.wait(settings.poll_interval):
            try:
                self.reconcile()
                run = self.check.claim_next_run(self.worker_id)
                if run is None:
                    continue
                self.verifier.run(run)
            except Exception:  # noqa: BLE001 - worker must never die
                logger.exception("worker loop iteration failed")
                time.sleep(settings.poll_interval)
