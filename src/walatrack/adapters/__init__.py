import types as _types

from importlib import import_module
from typing import TYPE_CHECKING

from .pandas_adapter import PandasAdapter

__all__: list[str] = ["PandasAdapter", "optional_lazy_adapter"]


def __getattr__(name: str) -> _types.ModuleType:
    if name == "optional_lazy_adapter":
        mod = import_module(f"{__name__}.optional_lazy_adapter")
        globals()[name] = mod
        return mod
    raise AttributeError(name)


if TYPE_CHECKING:  # pragma: no cover
    from . import spark_adapter
