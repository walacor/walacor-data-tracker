import pandas as pd
import pytest

from walacor_data_tracker.adapters.pandas_adapter import PandasAdapter
from walacor_data_tracker.core.tracker import Tracker


def test_init_creates_snapshot(tracker, pandas_adapter):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    # Exactly one new snapshot and it matches the DataFrame
    assert len(tracker.history) == 1
    snap = tracker.history[-1]
    assert snap.operation == "DataFrame.__init__"
    assert snap.shape == df.shape


def test_stop_unpatches_methods(tracker):
    original_merge = pd.DataFrame.merge
    adapter = PandasAdapter().start(tracker)
    assert pd.DataFrame.merge is not original_merge

    adapter.stop()
    assert pd.DataFrame.merge is original_merge


@pytest.fixture(scope="session")
def tracker():
    """Start a single Tracker + PandasAdapter for the whole test session."""
    tr = Tracker().start()
    adapter = PandasAdapter().start(tr)
    yield tr
    adapter.stop()
    tr.stop()


def make_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": [1, None, 3],
            "key": [1, 2, 3],
            "group": ["x", "y", "x"],
            "val": [10, 20, 30],
        }
    )


def _assert_snapshot(
    tr, before_count: int, op_suffix: str, expected_shape: tuple[int, int]
):
    assert len(tr.history) == before_count + 1, "No snapshot created"
    snap = list(tr.history)[-1]
    assert snap.operation.endswith(
        op_suffix
    ), f"Expected operation to end with {op_suffix}, got {snap.operation}"
    assert (
        snap.shape == expected_shape
    ), f"Shape mismatch: {snap.shape} vs {expected_shape}"


def test_copy_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.copy()

    _assert_snapshot(tracker, before, "copy", res.shape)
    assert res is not df


def test_fillna_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.fillna(0)

    _assert_snapshot(tracker, before, "fillna", res.shape)
    assert res.isna().sum().sum() == 0


def test_dropna_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.dropna()

    _assert_snapshot(tracker, before, "dropna", res.shape)
    assert res.isna().sum().sum() == 0


def test_replace_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.replace(1, 99)

    _assert_snapshot(tracker, before, "replace", res.shape)
    assert 1 not in res.values


def test_rename_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.rename(columns={"a": "alpha"})

    _assert_snapshot(tracker, before, "rename", res.shape)
    assert "alpha" in res.columns and "a" not in res.columns


def test_assign_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.assign(b=lambda d: d["a"].fillna(0) * 2)

    _assert_snapshot(tracker, before, "assign", res.shape)
    assert "b" in res.columns


def test_merge_snapshot(tracker):
    df = make_df()
    right = pd.DataFrame({"key": [1], "val2": ["x"]})
    before = len(tracker.history)

    res = df.merge(right, on="key", how="left")

    _assert_snapshot(tracker, before, "merge", res.shape)
    assert "val2" in res.columns


def test_join_snapshot(tracker):
    df = make_df()
    other = pd.DataFrame({"b": [10, 20, 30]}, index=df.index)
    before = len(tracker.history)

    res = df.join(other)

    _assert_snapshot(tracker, before, "join", res.shape)
    assert "b" in res.columns


def test_set_axis_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)
    new_cols = ["c1", "c2", "c3", "c4"]

    res = df.set_axis(new_cols, axis=1)

    _assert_snapshot(tracker, before, "set_axis", res.shape)
    assert list(res.columns) == new_cols


def test_reset_index_snapshot(tracker):
    df = make_df().set_index("key")
    before = len(tracker.history)

    res = df.reset_index()

    _assert_snapshot(tracker, before, "reset_index", res.shape)
    assert "key" in res.columns


def test_pivot_table_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.pivot_table(values="val", index="group", aggfunc="mean")

    _assert_snapshot(tracker, before, "pivot_table", res.shape)
    assert "val" in res.columns


def test_setitem_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    df["new"] = df["a"].fillna(0) * 3

    _assert_snapshot(tracker, before, "__setitem__", df.shape)
    assert "new" in df.columns


def test_insert_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    # insert mutates in‑place, returns None
    df.insert(0, "z", df["a"].fillna(0))

    _assert_snapshot(tracker, before, "insert", df.shape)
    assert "z" in df.columns and df.columns[0] == "z"


def test_astype_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.astype({"a": "float64"})

    _assert_snapshot(tracker, before, "astype", res.shape)
    assert res["a"].dtype == "float64"


def test_sort_values_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.sort_values("val", ascending=False)

    _assert_snapshot(tracker, before, "sort_values", res.shape)
    assert res.iloc[0]["val"] >= res.iloc[-1]["val"]


def test_reindex_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    new_order = [2, 0, 1]
    res = df.reindex(new_order)

    _assert_snapshot(tracker, before, "reindex", res.shape)
    assert list(res.index) == new_order


def test_explode_snapshot(tracker):
    df = pd.DataFrame({"A": [[1, 2], [3], []], "B": [1, 2, 3]})
    before = len(tracker.history)

    res = df.explode("A")

    _assert_snapshot(tracker, before, "explode", res.shape)
    assert len(res) >= len(df)


def test_melt_snapshot(tracker):
    df = make_df()
    before = len(tracker.history)

    res = df.melt(id_vars=["key"], value_vars=["a", "val"])

    _assert_snapshot(tracker, before, "melt", res.shape)
    assert {"variable", "value"}.issubset(res.columns)
