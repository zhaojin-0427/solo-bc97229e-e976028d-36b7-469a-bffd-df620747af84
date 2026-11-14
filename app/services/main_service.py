"""Application service: orchestration between HTTP layer, repos and queue."""
from __future__ import annotations

import json
from typing import Optional, Tuple

from ..models import Conflict, RunStatus
from ..repositories.business import BusinessRepository
from ..repositories.check import CheckRepository
from .csv_parse import GroupBatch, parse_groups, parse_relations


class LeakCheckService:
    def __init__(self) -> None:
        self.business = BusinessRepository()
        self.check = CheckRepository()

    # --- uploads ----------------------------------------------------------

    def upload_relations(self, raw: bytes, max_bytes: int) -> dict:
        edges = parse_relations(raw, max_bytes)
        result = self.business.add_relations(edges)
        run_id = self.check.enqueue(result["data_version"])
        result["change_type"] = "relation"
        result["run_id"] = run_id
        return result

    def upload_groups(self, raw: bytes, max_bytes: int) -> dict:
        batch: GroupBatch = parse_groups(raw, max_bytes)
        result = self.business.replace_groups(batch.assignments)
        result["duplicate_rows_merged"] = batch.duplicate_count
        run_id = self.check.enqueue(result["data_version"])
        result["change_type"] = "groups"
        result["run_id"] = run_id
        return result

    # --- status -----------------------------------------------------------

    def run_status(
        self, run_id: Optional[int] = None, with_conflicts: bool = True
    ) -> Optional[RunStatus]:
        row = (
            self.check.get_run(run_id)
            if run_id is not None
            else self.check.latest_run()
        )
        if row is None:
            return None
        return self._to_run_status(row, with_conflicts)

    def current_status(self) -> dict:
        edges, samples = self.business.counts()
        acceptance = self.business.acceptance()
        latest = self.check.latest_run()
        accepted_version = (
            int(acceptance["accepted_version"]) if acceptance else None
        )
        download_available = acceptance is not None
        return {
            "current_data_version": self.business.current_version(),
            "accepted_version": accepted_version,
            "acceptance_active": acceptance is not None,
            "latest_run": self._to_run_status(latest, True) if latest else None,
            "download_available": download_available,
            "download_version": accepted_version,
            "relation_edges": edges,
            "grouped_samples": samples,
        }

    # --- download ---------------------------------------------------------

    def download_artifact(self) -> Tuple[str, int, int]:
        """Return (file_path, row_count, version) of the frozen passed CSV."""
        acceptance = self.business.acceptance()
        if acceptance is None:
            return None  # caller maps to 409
        version = int(acceptance["accepted_version"])
        artifact = self.business.artifact(version)
        return artifact["file_path"], int(artifact["row_count"]), version

    # --- mapping ----------------------------------------------------------

    def _to_run_status(self, row, with_conflicts: bool) -> RunStatus:
        conflicts = None
        if with_conflicts and row["status"] == "failed":
            conflicts = [
                Conflict(
                    sample_a=c["sample_a"],
                    group_a=c["group_a"],
                    sample_b=c["sample_b"],
                    group_b=c["group_b"],
                    component_size=c["component_size"],
                    path=json.loads(c["path_json"]),
                )
                for c in self.check.conflicts_for(row["id"])
            ]
        return RunStatus(
            run_id=row["id"],
            data_version=row["data_version"],
            status=row["status"],
            progress_pct=row["progress_pct"],
            vertices_total=row["vertices_total"],
            vertices_processed=row["vertices_processed"],
            edges_total=row["edges_total"],
            components_total=row["components_total"],
            conflicts_total=row["conflicts_total"],
            attempts=row["attempts"],
            error_message=row["error_message"],
            queued_at=row["queued_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            conflicts=conflicts,
        )
