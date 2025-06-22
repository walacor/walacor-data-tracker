from importlib import import_module
from typing import TYPE_CHECKING
import types as _types


from .core.events import global_bus as event_bus    
from .core.events import global_bus                  
from .core.tracker import Tracker
from .core.snapshot import Snapshot
from .core.history  import History


from .adapters.pandas_adapter import PandasAdapter

__all__: list[str] = [
    "event_bus",          
    "global_bus",         
    "Tracker",
    "Snapshot",
    "History",
    "PandasAdapter",
    "writers",
    "adapters",
]

def __getattr__(name: str) -> _types.ModuleType:
    if name in {"writers", "adapters"}:
        mod = import_module(f"{__name__}.{name}")
        globals()[name] = mod         
        return mod
    raise AttributeError(name)


if TYPE_CHECKING:             # pragma: no cover
    from . import adapters, writers  # noqa: F401
