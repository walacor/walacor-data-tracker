from importlib import import_module
from typing import TYPE_CHECKING
import types as _types

__all__: list[str] = ["WalacorWriter", "walacor_client"]

def __getattr__(name: str) -> _types.ModuleType:
    if name in __all__:
        mod = import_module(f"{__name__}.{name}")
        globals()[name] = mod          # cache
        return mod
    raise AttributeError(name)

if TYPE_CHECKING:           # pragma: no cover
    from . import WalacorWriter, walacor_client   # noqa: F401
