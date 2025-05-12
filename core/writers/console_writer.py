from __future__ import annotations

from core.tracker import Snapshot

from ..events import global_bus


class ConsoleWriter:
    """Subscribe to *snapshot.created* and print a one‑liner."""
    def __init__(self) -> None:
        self._unsub = global_bus.subscribe(
            "snapshot.created", self._on_snapshot  # type: ignore[arg-type]
        )

    # ---------------------------------------------------------------- #
    def _on_snapshot(self, snapshot: Snapshot):  # noqa: ANN001 – generic handler
        print(
            f"[walatrack] {snapshot.timestamp}  {snapshot.operation:25s}"
            f"  {snapshot.shape}  {snapshot.artifact}"
        )

    def close(self) -> None:
        """Manually detach."""
        self._unsub()
