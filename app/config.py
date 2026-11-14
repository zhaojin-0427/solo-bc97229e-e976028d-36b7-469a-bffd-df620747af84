"""Runtime configuration.

All paths are configurable through environment variables so the same image can
be used by both API replicas (they point at the same mounted data directory).
"""
from __future__ import annotations

import os


def _data_dir() -> str:
    return os.environ.get("LEAKCHECK_DATA_DIR", "/data")


class Settings:
    """Plain mutable settings container (tests point it at temp dirs)."""

    def __init__(self) -> None:
        self.data_dir = _data_dir()
        self.business_db = os.environ.get(
            "LEAKCHECK_BUSINESS_DB", os.path.join(self.data_dir, "business.db")
        )
        self.check_db = os.environ.get(
            "LEAKCHECK_CHECK_DB", os.path.join(self.data_dir, "check.db")
        )
        self.snapshot_dir = os.environ.get(
            "LEAKCHECK_SNAPSHOT_DIR", os.path.join(self.data_dir, "snapshots")
        )
        # Maximum number of conflict details retained per verification run.
        self.max_conflicts = int(os.environ.get("LEAKCHECK_MAX_CONFLICTS", "50"))
        # Scheduler polling interval in seconds.
        self.poll_interval = float(os.environ.get("LEAKCHECK_POLL_INTERVAL", "1.0"))
        # A run whose heartbeat is older than this (seconds) is considered dead.
        self.heartbeat_timeout = float(
            os.environ.get("LEAKCHECK_HEARTBEAT_TIMEOUT", "30")
        )
        self.max_attempts = int(os.environ.get("LEAKCHECK_MAX_ATTEMPTS", "3"))
        # Max accepted CSV upload size (bytes).
        self.max_upload_bytes = int(
            os.environ.get("LEAKCHECK_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024))
        )


settings = Settings()
