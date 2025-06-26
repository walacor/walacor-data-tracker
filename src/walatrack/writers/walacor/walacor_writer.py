from __future__ import annotations

import logging
import threading

from typing import Any, Callable

from walatrack import event_bus
from walatrack.core.snapshot import Snapshot

from .walacor_client import WalacorClient

_LOG = logging.getLogger(__name__)
_EVENT = "snapshot.created"


class WalacorWriter:
    """
    High-level helper that turns every `snapshot.created` event into
    an immutable Walacor write:

        * artefact upload
        * transform_node row
        * transform_edge row (links from previous node)

    It is **thread-safe** (uses an RLock) and idempotent; call `close()`
    when the writer is no longer needed.
    """

    # ------------------------------------------------------------------ ctor
    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        *,
        project_name: str,
        description: str | None = None,
        user_tag: str | None = None,
    ) -> None:
        # -- low-level client façade -------------------------------------
        self._cli = WalacorClient(
            server=server,
            username=username,
            password=password,
            project_name=project_name,
            description=description,
            user_tag=user_tag,
        )

        # -- lineage state (per writer instance) -------------------------
        self._lock = threading.RLock()
        self._last_node_uid: str | None = None

        # -- subscribe to the global bus ---------------------------------
        self._unsubscribe: Callable[[], None] = event_bus.subscribe(
            _EVENT, self._on_snapshot_created
        )

    # ------------------------------------------------------------------ public
    def close(self) -> None:  # noqa: D401 – imperative
        """Detach from the global event bus (safe to call multiple times)."""
        try:
            self._unsubscribe()
        except Exception:  # pragma: no cover
            pass

    # ------------------------------------------------------------------ event handler
    def _on_snapshot_created(self, snapshot: Snapshot, **_: Any) -> None:
        """
        Persist *snapshot* and automatically create an edge from the
        previous node (if any) → this node, keeping full lineage intact.
        """
        try:
            with self._lock:
                new_uid = self._cli.insert_row(snapshot, parent_uid=self._last_node_uid)
                self._last_node_uid = new_uid
        except Exception:
            _LOG.exception("Failed to write snapshot to Walacor")
