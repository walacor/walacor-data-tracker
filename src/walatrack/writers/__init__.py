from importlib import import_module
from typing import TYPE_CHECKING
import types as _types

from .console.console_writer import ConsoleWriter  # light import

__all__: list[str] = [
    "ConsoleWriter",
    "walacor",          
]

def __getattr__(name: str) -> _types.ModuleType:
    if name == "walacor":
        mod = import_module(f"{__name__}.walacor")
        globals()[name] = mod
        return mod
    raise AttributeError(name)

if TYPE_CHECKING:          # pragma: no cover
    from . import walacor   # noqa: F401
