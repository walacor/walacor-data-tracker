from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from walacor_data_tracker import event_bus
from walacor_data_tracker.core.snapshot import Snapshot

from .walacor_client import WalacorClient

_LOG = logging.getLogger(__name__)
_EVENT = "snapshot.created"


class WalacorWriter:
    """
    High-level helper that turns every `snapshot.created` event into Walacor rows:
      • artefact upload
      • transform_node row
      • transform_edge row (links to previous node)
    Use `begin_run()` / `close()` to delimit a pipeline run.
    Thread-safe via RLock.
    """

    # ---------------------------------------------------------------- ctor
    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        *,
        project_name: str,
        description: str | None = None,
        user_tag: str | None = None,
        pipeline_name: str | None = None,  # convenience shortcut
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

        # subscribe to global bus
        self._unsubscribe: Callable[[], None] = event_bus.subscribe(
            _EVENT, self._on_snapshot_created
        )

        # optional auto-begin for quick scripts
        if pipeline_name:
            self.begin_run(pipeline_name)

    # ------------------------------------------------------------- run API
    def begin_run(self, pipeline_name: str, *, run_uid: str | None = None) -> str:
        """
        Start a logical pipeline run and return its UID.
        """
        with self._lock:
            if self._run_uid is not None:
                raise RuntimeError("Run already started for this writer")

            if run_uid is not None:
                self._run_uid = run_uid
            else:
                self._run_uid = self._cli.ensure_run_row(
                    pipeline_name=pipeline_name,
                    status="running",
                )
            return self._run_uid

    def close(self, *, status: str = "finished") -> None:  # noqa: D401 – imperative
        """
        Detach from the event bus **and** mark the run's final status.
        Safe to call multiple times.
        """
        with self._lock:
            try:
                if self._run_uid:
                    self._cli.update_run_status(status)
            finally:
                try:
                    self._unsubscribe()
                except Exception:  # pragma: no cover
                    pass

    # ------------------------------------------------------------- handler
    def _on_snapshot_created(self, snapshot: Snapshot, **_: Any) -> None:
        """
        Persist *snapshot* and automatically create an edge from the
        previous node (if any) → this node, preserving full lineage.
        """
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
            # best-effort: mark the run failed once, then ignore further updates
            with self._lock:
                if self._run_uid:
                    try:
                        self._cli.update_run_status("failed")
                    finally:
                        self._run_uid = None
