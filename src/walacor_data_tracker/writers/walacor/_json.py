from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

JsonPrimitive = str | int | float | bool | None
JsonType = JsonPrimitive | list["JsonType"] | dict[str, "JsonType"]


def jsonify(obj: Any) -> JsonType:
    if isinstance(obj, pd.Series):
        return cast(JsonType, obj.tolist())

    if isinstance(obj, pd.DataFrame):
        return cast(JsonType, obj.to_dict(orient="records"))

    if isinstance(obj, Mapping):
        return {k: jsonify(v) for k, v in obj.items()}

    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        return [jsonify(v) for v in obj]

    return cast(JsonType, obj)
