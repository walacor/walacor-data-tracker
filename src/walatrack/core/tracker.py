from __future__ import annotations

import threading

from hashlib import blake2b
from typing import Any, Iterable

from .events import global_bus
from .history import History
from .snapshot import Snapshot


class Tracker:
    """Create snapshots and broadcast them on the global event bus."""

    def __init__(self, max_history: int | None = None) -> None:
        self.history = History(max_len=max_history)
        self._running = False

        self._last_fp: dict[int, str] = {}
        self._fp_lock = threading.RLock()

    def start(self) -> "Tracker":
        self._running = True
        global_bus.publish("tracker.started")
        return self

    def stop(self) -> None:
        self._running = False
        global_bus.publish("tracker.stopped")

    @staticmethod
    def _make_fp(
        op: str,
        parents: tuple[str, ...],
        shape: tuple[int, ...] | None,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> str:
        h = blake2b(digest_size=16)
        h.update(op.encode())
        h.update(",".join(parents).encode())
        h.update(str(shape).encode())
        h.update(str(args).encode())
        h.update(str(sorted(kwargs.items())).encode())
        return h.hexdigest()

    def _idempotent_track(
        self,
        operation: str,
        artifact: Any,
        *args: Any,
        parents: Iterable[str] = (),
        **kwargs: Any,
    ) -> Snapshot | None:
        """Adapters call this → creates snapshot *unless* identical to last."""
        if not self._running:
            return None

        shape: tuple[int, ...] | None = getattr(artifact, "shape", None)
        if shape is None and hasattr(artifact, "__len__"):
            shape = (len(artifact),)

        parent_ids = tuple(str(p) for p in parents)
        fp = self._make_fp(operation, parent_ids, shape, args, kwargs)
        oid = id(artifact)

        with self._fp_lock:
            if self._last_fp.get(oid) == fp:
                return None
            self._last_fp[oid] = fp

        return self.track(operation, artifact, *args, parents=parent_ids, **kwargs)

    def track(
        self,
        operation: str,
        artifact: Any,
        *args: Any,
        parents: Iterable[str] = (),
        **kwargs: Any,
    ) -> Snapshot | None:
        """Low-level snapshot creator (adapters should use `_idempotent_track`)."""
        if not self._running:
            return None

        shape: tuple[int, ...] | None = getattr(artifact, "shape", None)
        if shape is None and hasattr(artifact, "__len__"):
            shape = (len(artifact),)

        snap = Snapshot(
            operation=operation,
            shape=shape,
            parents=tuple(parents),
            args=args,
            kwargs=kwargs,
            artifact=artifact,  # strong reference later we introduce weak copy
        )

        self.history.append(snap)
        global_bus.publish("snapshot.created", snapshot=snap)
        return snap

    def manual(self, note: str, artifact: Any) -> Snapshot | None:
        return self._idempotent_track(note, artifact)
