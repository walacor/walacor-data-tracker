from walatrack import event_bus
from walatrack.core.snapshot import Snapshot


class ConsoleWriter:
    """Subscribe to *snapshot.created* and print a one-liner."""

    def __init__(self) -> None:
        # Subscribe once; keep the unsubscribe callback
        self._unsub = event_bus.subscribe("snapshot.created", self._on_snapshot)

    def _on_snapshot(self, snapshot: Snapshot) -> None:
        ts = snapshot.timestamp
        op = snapshot.operation
        shp = str(snapshot.shape) if snapshot.shape else "-"
        parents = ",".join(snapshot.parents) or "<root>"

        artifact = snapshot.artifact
        print(
            f"[walatrack] {ts}  {op:<25s}  {shp:<15s}  parents={parents:<25s}  {artifact!r}"
        )

    def close(self) -> None:
        """Detach the writer so it stops printing."""
        self._unsub()
