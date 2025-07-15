from __future__ import annotations

from typing import Any

JsonPrimitive = str | int | float | bool | None
JsonType = JsonPrimitive | list["JsonType"] | dict[str, "JsonType"]


def jsonify(obj: Any) -> JsonType:

    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    if callable(obj):
        return getattr(obj, "__name__", "<lambda>")

    if isinstance(obj, dict):
        return {k: jsonify(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [jsonify(v) for v in obj]

    return str(obj)
