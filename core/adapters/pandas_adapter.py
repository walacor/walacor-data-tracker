import functools
from core.adapters.base import BaseAdapter
from pandas.core.groupby.generic import DataFrameGroupBy
from core.tracker import Tracker
import pandas as pd  
import threading

_IN_COPY = threading.local()


class PandasAdapter(BaseAdapter):
    """Capture newly-created * mutated DataFrames."""

    _DF_METHODS: list[str] = [
        "__setitem__",  
        "fillna",
        "dropna",
        "replace",
        "rename",
        "assign",
        "merge",
        "sort_values",
        "drop", 
    ]

    _originals: dict[tuple[object, str], any] = {}


    def _wrap(self,method_name: str, tracker:Tracker):
        target = pd.DataFrame
        original = getattr(target, method_name)
        self._originals[(target, method_name)] = original

        @functools.wraps(original)
        def wrapper(df_self, *a, **kw):
            result = original(df_self, *a, **kw)

            target = result if isinstance(result, pd.DataFrame) else df_self
            tracker.track(f"DataFrame.{method_name}", target, *a, **kw)
            return result
        
        setattr(pd.DataFrame, method_name, wrapper)

    def _wrap_df_init(self, tracker: Tracker) -> None:
        original_init = pd.DataFrame.__init__
        self._originals[(pd.DataFrame, "__init__")] = original_init

        @functools.wraps(original_init)
        def init_wrapper(df_self, *a, **kw):
            original_init(df_self, *a, **kw)

            # defer the expensive/verbose work so the debugger
            # won’t try to repr() an incompletely-built frame
            def _log():
                tracker.track("DataFrame.__init__", df_self, *a, **kw)

            threading.Timer(0, _log).start()   # executes right after return

        pd.DataFrame.__init__ = init_wrapper

    def _wrap_copy(self, tracker: Tracker) -> None:
        original = pd.DataFrame.copy
        self._originals["copy"] = original

        @functools.wraps(original)
        def wrapper(df_self, *a, **kw):
            # already inside a copy that is being tracked – do the real work
            if getattr(_IN_COPY, "flag", False):
                return original(df_self, *a, **kw)

            _IN_COPY.flag = True                 # turn the guard *on*
            try:
                result = original(df_self, *a, **kw)
                tracker.track("DataFrame.copy", result, *a, **kw)
            finally:
                _IN_COPY.flag = False            # turn it off afterwards

            return result

        pd.DataFrame.copy = wrapper


    
    def _wrap_columns_setattr(self, tracker: Tracker) -> None:
        original = pd.DataFrame.__setattr__
        self._originals["__setattr__"] = original

        @functools.wraps(original)
        def wrapper(df_self, name, value):
            # let pandas do its normal work first
            original(df_self, name, value)

            # Track ONLY safe, user-visible column updates
            if (
                name == "columns"                       # we really care about this
                and hasattr(df_self, "_mgr")            # frame fully initialised
            ):
                tracker.track("DataFrame.columns", df_self, name, value)

        pd.DataFrame.__setattr__ = wrapper
    
    def _wrap_groupby(self, name: str, tracker: Tracker):
        gb_cls = DataFrameGroupBy
        original = getattr(gb_cls, name)
        self._originals[(DataFrameGroupBy, name)] = original

        @functools.wraps(original)
        def wrapper(gb_self, *a, **kw):
            result = original(gb_self, *a, **kw)         
            tracker.track(f"GroupBy.{name}", result, *a, **kw)
            return result

        setattr(gb_cls, name, wrapper)


    def _patch(self, tracker: Tracker) -> None:
        self._wrap_df_init(tracker)
        for name in self._DF_METHODS:
           self._wrap(name, tracker)
        self._wrap_copy(tracker) 
        self._wrap_columns_setattr(tracker)
        for m in ("ffill", "bfill"):
            self._wrap_groupby(m, tracker)


    def _unpatch(self) -> None:
        for (obj, attr), fn in self._originals.items():
          setattr(obj, attr, fn)
        self._originals.clear()