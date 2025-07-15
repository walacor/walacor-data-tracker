from __future__ import annotations

import logging
import threading

from typing import Any, Callable, Dict, List

from walacor_data_tracker import event_bus
from walacor_data_tracker.core.snapshot import Snapshot

from .walacor_client import WalacorClient

_LOG = logging.getLogger(__name__)
_EVENT = "snapshot.created"
JsonRow = Dict[str, Any]
JsonRows = List[JsonRow]


class WalacorWriter:
    """
    High-level helper that turns every `snapshot.created` event into:
      • artefact upload
      • transform_node row
      • transform_edge row
    """

    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        *,
        project_name: str,
        description: str | None = None,
        user_tag: str | None = None,
        pipeline_name: str | None = None,
    ) -> None:
        self._cli = WalacorClient(
            server=server,
            username=username,
            password=password,
            project_name=project_name,
            description=description,
            user_tag=user_tag,
        )

        self._lock = threading.RLock()
        self._last_node_uid: str | None = None
        self._run_uid: str | None = None

        self._unsubscribe: Callable[[], None] = event_bus.subscribe(
            _EVENT, self._on_snapshot_created
        )

        if pipeline_name:
            self.begin_run(pipeline_name)

    def begin_run(self, pipeline_name: str, *, run_uid: str | None = None) -> str:
        with self._lock:
            if self._run_uid:
                raise RuntimeError("Run already started for this writer")

            self._run_uid = run_uid or self._cli.ensure_run_row(
                pipeline_name=pipeline_name, status="running"
            )
            return self._run_uid

    def close(self, *, status: str = "finished") -> None:
        with self._lock:
            try:
                if self._run_uid:
                    self._cli.update_run_status(status)
            finally:
                try:
                    self._unsubscribe()
                except Exception:
                    pass

    def _on_snapshot_created(self, snapshot: Snapshot, **_: Any) -> None:
        try:
            with self._lock:
                new_uid = self._cli.insert_row(
                    snapshot,
                    parent_uid=self._last_node_uid,
                    run_uid=self._run_uid,
                )
                self._last_node_uid = new_uid
        except Exception:
            _LOG.exception("Failed to write snapshot to Walacor")
            with self._lock:
                if self._run_uid:
                    try:
                        self._cli.update_run_status("failed")
                    finally:
                        self._run_uid = None

    def get_projects(self) -> List[JsonRow]:
        return self._cli.list_projects()

    def get_pipelines(self) -> List[str]:
        return self._cli.list_pipelines()

    def get_pipelines_for_project(self, *args: Any, **kw: Any) -> List[str]:
        return self._cli.list_pipelines_for_project(*args, **kw)

    def get_runs(self, *args: Any, **kw: Any) -> JsonRows:
        return self._cli.list_runs(*args, **kw)

    def get_projects_with_pipelines(self) -> List[JsonRow]:
        return self._cli.list_projects_with_pipelines()

    def get_nodes(self, *args: Any, **kw: Any) -> JsonRows:
        return self._cli.list_nodes(*args, **kw)

    def get_dag(self, *args: Any, **kw: Any) -> Dict[str, JsonRows]:
        return self._cli.list_dag(*args, **kw)
