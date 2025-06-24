import types as _types

from importlib import import_module
from typing import TYPE_CHECKING

from .walacor_writer import WalacorWriter

__all__: list[str] = ["WalacorWriter", "walacor_client"]


def __getattr__(name: str) -> _types.ModuleType:
    if name == "walacor_client":
        mod = import_module(f"{__name__}.walacor_client")
        globals()[name] = mod
        return mod
    raise AttributeError(name)


if TYPE_CHECKING:  # pragma: no cover
    from . import walacor_client
