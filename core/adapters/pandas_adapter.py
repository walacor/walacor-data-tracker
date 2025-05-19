from __future__ import annotations

import contextvars
import functools
import threading
from typing import Any

import pandas as pd

from core.adapters.base import BaseAdapter
from core.tracker       import Tracker


# Re-entrancy flag (True while we’re already inside a patched pandas call)
_inside_call = contextvars.ContextVar("_inside_call", default=False)


class PandasAdapter(BaseAdapter):
    """
    Monkey-patch selected DataFrame mutators so each *user-level* transformation
    produces exactly one snapshot.  Internal helper calls are skipped via the
    `_inside_call` flag.
    """

    _DF_METHODS: list[str] = [
    # creators that often run *inside* other ops
    "copy", "pivot_table", "reset_index",

    # mutators / aggregators
    "__setitem__", "fillna", "dropna", "replace", "rename",
    "assign", "merge", "join","set_axis"
    ]

    def __init__(self) -> None:
        super().__init__()
        self._originals: dict[str, Any] = {}

        self._parent_of: dict[int, str] = {}
        self._objects:   dict[int, pd.DataFrame] = {}

        self._lock = threading.RLock()

    def _remember(self, df: pd.DataFrame, snap_id: str) -> None:
        oid = id(df)
        self._parent_of[oid] = snap_id
        self._objects[oid]   = df  

    def _parent_id(self, df: pd.DataFrame) -> str | None:
        return self._parent_of.get(id(df))

    def _wrap_df_init(self, tracker: Tracker) -> None:
        original = pd.DataFrame.__init__
        self._originals["__init__"] = original

        @functools.wraps(original)
        def init_wrapper(df_self: pd.DataFrame, *a, **kw):
            if _inside_call.get():
                return original(df_self, *a, **kw)   # nested – ignore

            token = _inside_call.set(True)
            try:
                original(df_self, *a, **kw)
                snap = tracker._idempotent_track(
                    "DataFrame.__init__", df_self, *a, **kw, parents=()
                )
                if snap:
                    with self._lock:
                        self._remember(df_self, snap.id)
            finally:
                _inside_call.reset(token)

        pd.DataFrame.__init__ = init_wrapper

    def _wrap(self, name: str, tracker: Tracker) -> None:
        original = getattr(pd.DataFrame, name)
        self._originals[name] = original

        @functools.wraps(original)
        def wrapper(df_self: pd.DataFrame, *a, **kw):
            if _inside_call.get():
                return original(df_self, *a, **kw)   # nested – ignore

            token = _inside_call.set(True)
            try:
                parent = self._parent_id(df_self)
                result = original(df_self, *a, **kw)
                target = result if isinstance(result, pd.DataFrame) else df_self

                snap = tracker._idempotent_track(
                    f"DataFrame.{name}",
                    target,
                    *a,
                    **kw,
                    parents=(parent,) if parent else (),
                )
                if snap:
                    with self._lock:
                        self._remember(target, snap.id)
                return result
            finally:
                _inside_call.reset(token)

        setattr(pd.DataFrame, name, wrapper)

    def _patch(self, tracker: Tracker) -> None:
        self._wrap_df_init(tracker)
        for n in self._DF_METHODS:
            self._wrap(n, tracker)

    def _unpatch(self) -> None:
        for n, fn in self._originals.items():
            setattr(pd.DataFrame, n, fn)
        self._originals.clear()
        self._parent_of.clear()
        self._objects.clear()
