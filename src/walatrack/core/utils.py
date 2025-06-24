from __future__ import annotations

import datetime as _dt
import uuid

from typing import Any


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 with “Z” suffix."""
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .replace(tzinfo=_dt.timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def generate_uuid() -> str:
    """Shorthand for ``str(uuid.uuid4())``"""
    return str(uuid.uuid4())


def deepcopy_artifact(obj: Any) -> Any:
    """Attempt a deep copy of *obj* while **not** importing heavy libs up front.

    Priority order
    --------------

    1.  ``.copy(deep=True)``    – pandas, numpy
    2.  ``.copy()`` / ``.clone()`` – torch tensors / simple objects
    3.  ``copy.deepcopy``       – built-in containers
    4.  *Last resort*: return original object (mutable!)

    The function never raises; at worst you get the same reference back.
    """
    for method in ("copy", "clone"):
        fn = getattr(obj, method, None)
        if callable(fn):
            try:
                if "deep" in fn.__code__.co_varnames:
                    return fn(deep=True)  # pandas / numpy
                return fn()  # torch.clone() / generic copy()
            except TypeError:
                # signature mismatch – try next candidate
                continue
