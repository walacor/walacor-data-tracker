import functools
from core.adapters.base import BaseAdapter
from core.tracker import Tracker
import pandas as pd  
import threading


class PandasAdapter(BaseAdapter):
    """Capture newly-created * mutated DataFrames."""

    _DF_METHODS: list[str] = [
        "__setitem__",  
        "fillna",
        "dropna",
        "replace",
        "rename",
        "assign",
    ]

    _originals: dict[str,any]= {}


    def _wrap(self,method_name: str, tracker:Tracker):
        original = getattr(pd.DataFrame, method_name)
        self._originals[method_name]= original

        @functools.wraps(original)
        def wrapper(df_self, *a, **kw):
            result = original(df_self, *a, **kw)

            target = result if isinstance(result, pd.DataFrame) else df_self
            tracker.track(f"DataFrame.{method_name}", target, *a, **kw)
            return result
        
        setattr(pd.DataFrame, method_name, wrapper)

    def _wrap_df_init(self, tracker:Tracker) -> None:
        original_init = pd.DataFrame.__init__
        self._originals["__init__"] = original_init

        @functools.wraps(original_init)
        def init_wrapper(df_self, *a, **kw):
            original_init(df_self,*a, **kw)
            tracker.track("Dataframe.__init__", df_self, *a,**kw)

        pd.DataFrame.__init__= init_wrapper

    def _patch(self, tracker: Tracker) -> None:
        self._wrap_df_init(tracker)
        for name in self._DF_METHODS:
            self._wrap(name, tracker)

    def _unpatch(self) -> None:
        for name, fn in self._originals.items():
            setattr(pd.DataFrame, name, fn)
        self._originals.clear()