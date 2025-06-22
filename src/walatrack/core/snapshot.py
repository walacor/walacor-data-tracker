from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

from .utils import generate_uuid, utc_now_iso 

@dataclass(frozen=True, slots=True)
class Snapshot:
    """Immutable capture of an artifact **after** a transformation.

    Attributes
    ----------
    operation
        Human‑friendly label – usually the pandas/NumPy/sklearn method name the
        patcher intercepted (e.g. ``DataFrame.merge``).
    shape
        Tuple containing *rows, columns[, ...]* depending on the artifact type.
    parents
        Tuple of **snapshot ids** this artifact immediately depends on.  Empty
        for source artifacts (e.g. read_csv()).  Using *ids* keeps the object
        small and avoids reference cycles.
    args / kwargs
        Raw positional / keyword arguments captured from the original call –
        optional, but handy for provenance queries.
    artifact
        Optional in‑memory pointer to the concrete object.  We keep it because
        you already rely on it, but anything heavy (e.g. a full DataFrame) is
        better off being stored externally and replaced here with a thin proxy
        or a UID.
    """

    operation: str
    shape: Tuple[int, ...]
    parents: Tuple[str, ...] = field(default_factory=tuple, repr=False)
    args: Tuple[Any, ...] = field(default_factory=tuple, repr=False)
    kwargs: Dict[str, Any] = field(default_factory=dict, repr=False)
    artifact: Any | None = field(default=None, repr=False)

    id: str = field(default_factory=generate_uuid, init=False)
    timestamp: str = field(default_factory=utc_now_iso, init=False)

    def __repr__(self) -> str:
        parent_lbl = ",".join(self.parents) if self.parents else "<root>"
        return (
            f"<Snapshot {self.timestamp} op={self.operation} shape={self.shape} "
            f"parents={parent_lbl}>"
        )

