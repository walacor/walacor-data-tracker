import collections
from typing import Deque, Dict, Iterator, List

from core.snapshot import Snapshot


class History:
    """A ring‑buffer of snapshots **plus** a lightweight DAG index."""

    def __init__(self, max_len: int | None = None) -> None:
        self._buf: Deque[Snapshot] = collections.deque(maxlen=max_len or 1_000)
        self._children: Dict[str, List[str]] = {}
        self._parents: Dict[str, List[str]] = {}

 
    def append(self, snap: Snapshot) -> None:
        """Append *snap* and update adjacency lists.

        If the buffer is at capacity and a *left* snapshot is evicted we *also*
        evict its edges to keep indices in sync.
        """
        evicted: Snapshot | None = None
        if self._buf.maxlen and len(self._buf) == self._buf.maxlen:
            evicted = self._buf[0]

        self._buf.append(snap)

        self._parents[snap.id] = list(snap.parents)
        for pid in snap.parents:
            self._children.setdefault(pid, []).append(snap.id)

        if evicted is not None:
            self._remove_snapshot(evicted.id)

    
    def parents_of(self, sid: str) -> List[str]:
        return self._parents.get(sid, [])

    def children_of(self, sid: str) -> List[str]:
        return self._children.get(sid, [])

    def ancestors_of(self, sid: str) -> Iterator[str]:
        stack = list(self.parents_of(sid))
        seen: set[str] = set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            yield current
            stack.extend(self.parents_of(current))

    def descendants_of(self, sid: str) -> Iterator[str]:
        stack = list(self.children_of(sid))
        seen: set[str] = set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            yield current
            stack.extend(self.children_of(current))

    def __len__(self) -> int:  # pragma: no cover – trivial
        return len(self._buf)

    def __iter__(self) -> Iterator[Snapshot]:  # pragma: no cover – trivial
        return iter(self._buf)

    def __getitem__(self, idx: int) -> Snapshot:  # pragma: no cover – list copy ok
        return list(self._buf)[idx]

    def filter(self, op: str | None = None) -> Iterator[Snapshot]:
        """Yield snapshots whose *operation* matches *op* (or all if *None*)."""
        for snap in self._buf:
            if op is None or snap.operation == op:
                yield snap

    def _remove_snapshot(self, sid: str) -> None:
        """Remove *sid* from adjacency lists – called when buffer evicts."""
        for p in self._parents.pop(sid, []):
            self._children.get(p, []).remove(sid)
            if not self._children[p]:  
                self._children.pop(p)
                
        self._children.pop(sid, None)

