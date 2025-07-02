from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

JsonPrimitive = str | int | float | bool | None
JsonType = JsonPrimitive | list["JsonType"] | dict[str, "JsonType"]


def jsonify(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    if callable(obj):
        return getattr(obj, "__name__", "<lambda>")

    if isinstance(obj, dict):
        return {k: jsonify(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [jsonify(v) for v in obj]

    # fallback for objects that don’t match the above
    return str(obj)
