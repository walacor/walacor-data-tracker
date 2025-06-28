from pandas import Series, DataFrame
from collections.abc import Mapping, Sequence

def jsonify(obj):
    if isinstance(obj, Series):
        return obj.tolist()
    if isinstance(obj, DataFrame):
        return obj.to_dict(orient="records")
    if isinstance(obj, Mapping):
        return {k: jsonify(v) for k, v in obj.items()}
    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        return [jsonify(v) for v in obj]
    return obj
