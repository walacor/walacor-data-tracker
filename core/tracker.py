import collections
from dataclasses import dataclass, field
from typing import Any, Deque, Iterator, Tuple
from uuid import UUID
from .events import global_bus

from .utils import deepcopy_artifact, generate_uuid, utc_now_iso


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Immutable capture of a data artifact *after* a transformation."""

    operation: str                      
    shape: Tuple[int, ...]               
    args: Tuple[Any, ...] = field(default_factory=tuple, repr=False)
    kwargs: dict[str, Any] = field(default_factory=dict, repr=False)
    artifact: Any | None = field(default=None, repr=False)

    id: str = field(default_factory=generate_uuid, init=False)
    timestamp: str = field(default_factory=utc_now_iso, init=False)

    def __repr__(self) -> str:  
        return (
            f"<Snapshot {self.timestamp}, op={self.operation}, shape={self.shape}>"
        )

class History:
    def __init__(self,max_len: int | None =None)->None:
        self._buf: Deque[Snapshot] = collections.deque(maxlen= max_len or 1_000) # type: ignore
        
    def append(self, snap: Snapshot) -> None:
        self._buf.append(snap)

    def __len__(self) -> int: 
        return len(self._buf)
    
    def __iter__(self)->Iterator[Snapshot]:
        return iter(self._buf)
    
    def __getitem__(self, idx: int)->Snapshot:
        return list(self._buf)[idx]
    
    def filter(self, op:str | None = None)-> Iterator[Snapshot]:
        """Yield snapshots whose ''operation'' matches *op* (or all if *None*)."""
        for snap in self._buf:
            if op is None or snap.operation == op:
                yield snap


class Tracker:
    """Create snapshots and notify subscribers via global event bus"""
    
    def __init__(self,max_history: int | None = None)->None:
        self.history = History(max_len=max_history)
        self._running=False

    
    def start(self)->"Tracker":
        """Enable the tracker (idempotent)"""
        self._running=True
        global_bus.publish("tracker.started")
        return self
    
    def stop(self)->None:
        """Disable the tracker and broadcast a final event"""
        self._running=False
        global_bus.publish("tracker.stopped")

    def track(self, operation: str, artifact:Any, *a:Any, **kw: Any)->Snapshot | None:
        """Capture *artifact* after performing *operation*
        
        returns the created :class:`Snapshot` or ``None`` when the tracker 
        is not running (so adapters can remain silent).
        """

        if not self._running:
            return None
        
        shape = getattr(artifact, "shape",None)
        if shape is None and hasattr(artifact, "__len___"):
            shape = (len(artifact),)

        snap = Snapshot(
            operation=operation,
            shape=shape,
            args=a,
            kwargs=kw,
            artifact= deepcopy_artifact(artifact)
        )
        self.history.append(snap)
        global_bus.publish("snapshot.created", snapshot=snap)

    def manual(self, note:str, artifact:Any)->Snapshot| None:
        """Manually add a snapshot with a free-text *note*."""
        return self.track(note,artifact)