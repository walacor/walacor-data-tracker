"""
azure_pdm_pipeline.py
End-to-end predictive-maintenance pipeline (Azure PdM data).
"""

from __future__ import annotations
import logging, itertools, joblib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sklearn.model_selection import TimeSeriesSplit, cross_validate
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier

from core.adapters.pandas_adapter import PandasAdapter
from core.tracker import Tracker
from core.writers.console_writer import ConsoleWriter

# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CFG:
    data_dir : Path = Path.home() / "data"             
    files    : tuple[str, ...] = (
        "PdM_telemetry.csv",
        "PdM_errors.csv",
        "PdM_failures.csv",
        "PdM_maint.csv",
        "PdM_machines.csv",
    )
    ts_col   : str  = "datetime"                        
    id_col   : str  = "machineid"
    horizon_h: int  = 24
    cv_splits: int  = 5
    model_dir: Path = Path("artifacts")

# --------------------------------------------------------------------------- #
# 1) LOAD  &  NORMALISE  ----------------------------------------------------- #
def load_tables(cfg: CFG) -> dict[str, pd.DataFrame]:
    def _read(fname: str) -> pd.DataFrame:
        df = pd.read_csv(cfg.data_dir / fname)
        df.columns = df.columns.str.strip().str.lower()        # normalise once
        if cfg.ts_col in df.columns:
            df[cfg.ts_col] = pd.to_datetime(df[cfg.ts_col])
        return df
    return {fn: _read(fn) for fn in cfg.files}

# --------------------------------------------------------------------------- #
# 2) MERGE  &  LABEL  -------------------------------------------------------- #
def build_panel(tbl: dict[str, pd.DataFrame], cfg: CFG) -> pd.DataFrame:
    tel  = tbl["PdM_telemetry.csv"].rename(columns={"volt": "voltage"})
    err  = tbl["PdM_errors.csv"]
    fail = tbl["PdM_failures.csv"]
    mach = tbl["PdM_machines.csv"]

    # ---- error flags wide ---------------------------------------------------
    err["err_flag"] = 1
    err_wide = (
        err.pivot_table(index=[cfg.id_col, cfg.ts_col],
                        columns="errorid",
                        values="err_flag",
                        fill_value=0)
        .reset_index()
    )
    tel = tel.merge(err_wide, on=[cfg.id_col, cfg.ts_col], how="left").fillna(0)

    # ---- static machine info -----------------------------------------------
    tel = tel.merge(mach, on=cfg.id_col, how="left")

    # ---- binary label: failure within next 24 h -----------------------------
    fail = fail.copy()
    fail["label"] = 1
    shifted = fail[[cfg.id_col, cfg.ts_col, "label"]].copy()
    shifted[cfg.ts_col] -= pd.Timedelta(hours=cfg.horizon_h)

    tel = tel.merge(shifted, on=[cfg.id_col, cfg.ts_col], how="left")
    tel["label"] = tel["label"].fillna(0).astype(int)

    # ---- order & forward-fill tiny gaps -------------------------------------
    tel = tel.sort_values([cfg.id_col, cfg.ts_col])

    # Preserve machineid explicitly before groupby-ffill
    tel["__machineid"] = tel[cfg.id_col]

    tel = tel.groupby(cfg.id_col, group_keys=False).ffill(limit=3)
    tel[cfg.id_col] = tel["__machineid"]
    tel = tel.drop(columns="__machineid").reset_index(drop=True)

    logging.info("Merged panel shape: %s", tel.shape)
    return tel

# --------------------------------------------------------------------------- #
# 3) FEATURE ENGINEERING  ---------------------------------------------------- #
def engineer_features(df: pd.DataFrame, cfg: CFG):
    print("\n✅ DataFrame columns in engineer_features:\n", df.columns.tolist())

    sensors = ["voltage", "rotate", "pressure", "vibration"]

    rolled = (
        df.groupby(cfg.id_col)
          .rolling("3h", on=cfg.ts_col, min_periods=2)[sensors]
          .agg(["mean", "std"])
          .reset_index()
    )
    rolled.columns = [cfg.id_col, cfg.ts_col] + [
        f"{s}_{stat}" for s, stat in itertools.product(sensors, ("mean", "std"))
    ]

    df = df.merge(rolled, on=[cfg.id_col, cfg.ts_col])
    y  = df.pop("label").astype(int)
    X  = df.drop(columns=[cfg.ts_col])
    return X, y

# --------------------------------------------------------------------------- #
# 4) MODEL PIPELINE  --------------------------------------------------------- #
def build_model(num_cols: list[str], cat_cols: list[str]) -> Pipeline:
    num_pipe = Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    cat_pipe = Pipeline([
        ("imp",    SimpleImputer(strategy="most_frequent")),
        ("encode", OneHotEncoder(handle_unknown="ignore")),
    ])
    prep = ColumnTransformer([
        ("num", num_pipe, num_cols),
        ("cat", cat_pipe, cat_cols),
    ])
    clf = RandomForestClassifier(
        n_estimators=300,
        n_jobs=-1,
        class_weight="balanced",
        random_state=42,
    )
    return Pipeline([("prep", prep), ("clf", clf)])

# --------------------------------------------------------------------------- #
# 5) MAIN  ------------------------------------------------------------------- #
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    tracker = Tracker().start()
    PandasAdapter().start(tracker)
    ConsoleWriter()  
    
    cfg = CFG()

    tables = load_tables(cfg)
    panel  = build_panel(tables, cfg)
    X, y   = engineer_features(panel, cfg)

    num_cols = [c for c in X.columns if X[c].dtype != "object" and c != cfg.id_col]
    cat_cols = ["model"]

    pipe  = build_model(num_cols, cat_cols)
    tscv  = TimeSeriesSplit(n_splits=cfg.cv_splits)

    scores = cross_validate(pipe, X, y, cv=tscv,
                            scoring={"f1": "f1", "roc": "roc_auc"},
                            n_jobs=-1)
    logging.info("F1  (mean) : %.3f", scores["test_f1"].mean())
    logging.info("ROC AUC(mean): %.3f", scores["test_roc"].mean())

    pipe.fit(X, y)
    cfg.model_dir.mkdir(exist_ok=True)
    out = cfg.model_dir / "azure_pdm_rf.pkl"
    joblib.dump(pipe, out)
    logging.info("Model saved ➜ %s", out)

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
