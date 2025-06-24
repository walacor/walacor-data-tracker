from __future__ import annotations

"""walatrack.adapters.pandas_adapter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Adapter that monkey‑patches ``pandas.DataFrame`` so that *user‑level*
operations are automatically tracked by Walatrack.  Every mutating step
creates exactly **one** snapshot, while internal helper calls are skipped.
"""

import contextvars
import functools
import threading

from typing import Any, Callable

import pandas as pd

from .base import BaseAdapter

__all__ = ["PandasAdapter"]

# ---------------------------------------------------------------------------
# Module‑level state
# ---------------------------------------------------------------------------
_inside_call: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_inside_call", default=False
)


class PandasAdapter(BaseAdapter):
    """Patch a curated set of ``DataFrame`` methods so they emit snapshots."""

    # ---------------------------------------------------------------------
    # Configuration – keep this list small and focused!
    # ---------------------------------------------------------------------
    _DF_METHODS: list[str] = [
        # creators that often run *inside* other ops
        "copy",
        "pivot_table",
        "reset_index",
        # mutators / aggregators
        "__setitem__",
        "fillna",
        "dropna",
        "replace",
        "rename",
        "assign",
        "merge",
        "join",
        "set_axis",
    ]

    # ------------------------------------------------------------------
    # Construction / teardown
    # ------------------------------------------------------------------
    def __init__(self) -> None:  # noqa: D401
        super().__init__()

        self._originals: dict[str, Callable[..., Any]] = {}
        self._parent_of: dict[int, str] = {}
        self._objects: dict[int, pd.DataFrame] = {}

        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def _remember(self, df: pd.DataFrame, snap_id: str) -> None:
        """Remember the latest snapshot ID for *df* by object identity."""
        oid: int = id(df)
        self._parent_of[oid] = snap_id
        self._objects[oid] = df

    def _parent_id(self, df: pd.DataFrame) -> str | None:
        """Return the parent snapshot ID (if any) for *df*."""
        return self._parent_of.get(id(df))

    # ------------------------------------------------------------------
    # Monkey‑patch logic
    # ------------------------------------------------------------------
    def _wrap_df_init(self) -> None:
        """Patch ``DataFrame.__init__`` so construction is tracked."""
        original: Callable[..., None] = pd.DataFrame.__init__  # type: ignore[attr-defined]
        self._originals["__init__"] = original

        @functools.wraps(original)
        def init_wrapper(
            df_self: pd.DataFrame, *a: Any, **kw: Any
        ) -> None:  # noqa: ANN001
            if _inside_call.get():
                # Nested call – ignore to avoid multiple snapshots for one op
                return original(df_self, *a, **kw)

            token = _inside_call.set(True)
            try:
                original(df_self, *a, **kw)
                snap = self.tracker._idempotent_track(  # type: ignore[attr-defined]
                    "DataFrame.__init__", df_self, *a, **kw, parents=()
                )
                if snap:
                    with self._lock:
                        self._remember(df_self, snap.id)
            finally:
                _inside_call.reset(token)

        pd.DataFrame.__init__ = init_wrapper  # type: ignore[assignment]

    def _wrap(self, name: str) -> None:
        """Patch a single DataFrame method so it emits at most one snapshot."""
        original: Callable[..., Any] = getattr(pd.DataFrame, name)
        self._originals[name] = original

        @functools.wraps(original)
        def wrapper(df_self: pd.DataFrame, *a: Any, **kw: Any) -> Any:  # noqa: ANN001
            if _inside_call.get():
                return original(df_self, *a, **kw)

            token = _inside_call.set(True)
            try:
                parent = self._parent_id(df_self)
                result = original(df_self, *a, **kw)
                target: pd.DataFrame = (
                    result if isinstance(result, pd.DataFrame) else df_self
                )

                snap = self.tracker._idempotent_track(  # type: ignore[attr-defined]
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

    # ------------------------------------------------------------------
    # Public API required by ``BaseAdapter``
    # ------------------------------------------------------------------
    def _patch(self) -> None:  # noqa: D401, ANN001
        """Apply all monkey‑patches. Called by :pyclass:`BaseAdapter`."""
        # ``BaseAdapter`` ensures ``self.tracker`` exists before invoking us.
        self._wrap_df_init()
        for meth in self._DF_METHODS:
            self._wrap(meth)

    def _unpatch(self) -> None:  # noqa: D401
        """Restore all patched methods to their original state."""
        for name, fn in self._originals.items():
            setattr(pd.DataFrame, name, fn)

        self._originals.clear()
        self._parent_of.clear()
        self._objects.clear()
