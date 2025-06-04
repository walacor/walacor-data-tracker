from core.snapshot import Snapshot
from src.core.events import global_bus


class ConsoleWriter:
    """Subscribe to *snapshot.created* and print a one-liner."""
    def __init__(self) -> None:
        self._unsub = global_bus.subscribe(
            "snapshot.created", self._on_snapshot  # type: ignore[arg-type]
        )

    def _on_snapshot(self, snapshot: Snapshot):  # noqa: ANN001 – generic handler
        ts        = snapshot.timestamp
        op        = snapshot.operation
        shp       = str(snapshot.shape) if snapshot.shape else "-"
        parents   = ",".join(snapshot.parents) if snapshot.parents else "<root>"
        artifact  = snapshot.artifact
        print(type(artifact))
        print(
            f"[walatrack] {ts}  {op:<25s}  {shp:<15s}  parents={parents:<25s}  {artifact!r}"
        )

    def close(self) -> None:
        """Manually detach."""
        self._unsub()