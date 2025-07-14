# Walacor Data Tracking

<div align="center">

<img src="https://www.walacor.com/wp-content/uploads/2022/09/Walacor_Logo_Tag.png" width="300" />

[![License Apache 2.0][badge-license]][license]
[![Walacor (1100127456347832400)](https://img.shields.io/badge/My-Discord-%235865F2.svg?label=Walacor)](https://discord.gg/BaEWpsg8Yc)
[![Walacor (1100127456347832400)](https://img.shields.io/static/v1?label=Walacor&message=LinkedIn&color=blue)](https://www.linkedin.com/company/walacor/)
[![Walacor (1100127456347832400)](https://img.shields.io/static/v1?label=Walacor&message=Website&color)](https://www.walacor.com/product/)

</div>

[badge-license]: https://img.shields.io/badge/license-Apache2-green.svg?dummy
[license]: https://github.com/walacor/objectvalidation/blob/main/LICENSE

---



A schema-first framework to **track, version, and store the full lineage of data transformations** — from raw ingestion to final model output — using Walacor as a backend snapshot store.

---

## ✨ Why this exists
- **Reproducibility** – Every transformation, parameter, and artifact is captured in a graph you can replay.
- **Auditability** – Snapshots are immutable, version-controlled, and timestamped.
- **Collaboration** – Team members see the same lineage and can compare or branch workflows.
- **Extensibility** – Strict JSON-schemas keep today’s pipelines clean while allowing tomorrow’s to evolve safely.

---

## 🏗️ Core Concepts

| Concept | Stored as | Purpose |
| ------- | --------- | ------- |
| **Transform Node** | `transform_node` | One operation (e.g., “fit model”, “clean text”). |
| **Transform Edge** | `transform_edge` | Dependency between two nodes. |
| **Project Metadata** | `project_metadata` | Run-level info (owner, description, timestamps). |

> **Immutable Snapshots**
> Once a DAG is written to Walacor, it cannot mutate—only a *new* snapshot (with a higher SV or run ID) can supersede it.

---


## 🚀 Getting Started

### 1. Install the SDKs

```bash
pip install walatrack
````

> Make sure you're using Python 3.10+ and have internet access to reach the Walacor API.

### 2. Initialize the Tracking Components

To begin capturing your data lineage:

* **Start the Tracker** – This manages the session and records operations.
* **Attach an Adapter** – For example, use `PandasAdapter` to automatically track DataFrame transformations.
* **Add Writers** – Choose where to send the output:

  * Console output for quick inspection
  * WalacorWriter to persist snapshots to the Walacor backend

Once set up, your transformation history will be automatically recorded and can be exported or persisted.

---


## 🧪 Example Use Cases

* Track changes in a machine learning pipeline
* Visualize column-level transformations in pandas
* Record versions of a dataset as it’s cleaned and merged
* Keep an auditable log of automated workflows

---

Here’s the updated `README.md` with a concise, illustrative example that highlights how easy it is to use `walatrack`. This is placed right after the **Getting Started** section and demonstrates a realistic tracking flow with minimal code:

---

## 🧪 Minimal Example

Here's how simple it is to start tracking transformations:

```python
import pandas as pd
from walacor_data_tracker import Tracker, PandasAdapter
from walacor_data_tracker.writers import ConsoleWriter
from walacor_data_tracker.writers.walacor import WalacorWriter

# 1️⃣  Start tracking
tracker = Tracker().start()
PandasAdapter().start(tracker)        # auto-captures every DataFrame op
ConsoleWriter()                       # (optional) printf lineage to stdout

# 2️⃣  Open a Walacor run in one line
wal_writer = WalacorWriter(
    "https://your-walacor-url/api",    # server
    "your-username",                   # login
    "your-password",
    project_name="MyProject",
    pipeline_name="daily_sales_pipeline",   # ⇢ opens a new run right away
)

# 3️⃣  Do your normal pandas work
df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
df2 = df.assign(double=df.value * 2)
df3 = df2.rename(columns={"value": "v"})

# 4️⃣  Finish the run and stop tracking
wal_writer.close(status="finished")   # marks the run "finished" in Walacor
tracker.stop()

print("Walacor run UID:", wal_writer._run_uid)   # UID of the run you just wrote


````

> 💡 The `PandasAdapter` automatically tracks operations like `.assign()`, `.rename()`, `.merge()`, etc., so you can work with pandas as usual — but with versioned lineage behind the scenes.


---

This snippet:
- Is short enough to understand at a glance
- Avoids hardcoded credentials or IPs
- Clearly reflects your existing setup
- Shows the power and simplicity of the library

---


### 🛠️  Pandas operations automatically tracked

The current release wraps the pandas `DataFrame` API methods below.
Whenever you call any of them, a **transform \_node** is emitted, parameters are
captured, and lineage is updated—zero extra code required:

| Category                          | Supported `DataFrame` methods                                       |
| --------------------------------- | ------------------------------------------------------------------- |
| **Structural copies / reshaping** | `copy`, `reset_index`, `set_axis`, `pivot_table`, `melt`, `explode` |
| **Column creation / update**      | `assign`, `insert`, `__setitem__` (`df["col"] = …`)                 |
| **Cleaning & NA handling**        | `fillna`, `dropna`, `replace`                                       |
| **Column rename / re-order**      | `rename`, `reindex`, `sort_values`                                  |
| **Joins & merges**                | `merge`, `join`                                                     |
| **Type & dtype changes**          | `astype`                                                            |

> ℹ️ These map directly to the constant in `PandasAdapter`:
>
> ```python
> _DF_METHODS = [
>     "copy", "pivot_table", "reset_index", "__setitem__",
>     "fillna", "dropna", "replace", "rename", "assign",
>     "merge", "join", "set_axis", "insert", "astype",
>     "sort_values", "reindex", "explode", "melt",
> ]
> ```

#### Missing your favourite method?

Pull requests are welcome!
Add the method name to `_DF_METHODS`, ensure the wrapper captures a meaningful
snapshot, and open a PR. We’ll review and merge updates that keep to the
schema-first philosophy.

---
## 🔍 Helper API — query your lineage

| Helper                                                                        | Purpose                                                              | Key parameters                                                                                                                    | Returns                                                                           |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------   | --------------------------------------------------------------------------------- |
| `get_projects()`                                                              | List every Walacor-tracked project.                                  | *(none)*                                                                                                                          | `[{uid, project_name, description, user_tag}]`                                    |
| `get_pipelines()`                                                             | List the **names of all pipelines** ever executed (across projects). | *(none)*                                                                                                                          | `["daily_etl", "train_model", ...]`                                               |
| `get_pipelines_for_project(project_name, *, user_tag=None)`                   | Pipelines that belong to one project.                                | `project_name` – required<br>`user_tag` – filter if you store multiple laptops/branches                                           | `["sales_pipeline", …]`                                                           |
| `get_runs(project_name, *, pipeline_name=None, user_tag=None)`                | History of executions (“runs”).                                      | `project_name` – required<br>`pipeline_name` – limit to one pipeline<br>`user_tag` – optional                                     | `[{"UID","status","pipeline_name",…}, …]`                                         |
| `get_nodes(project_name, *, pipeline_name=None, run_uid=None, user_tag=None)` | Raw **transform\_node rows** (operations).                           | Same filters as above – *pick **one** of* `pipeline_name` **or** `run_uid`.<br>Omitting both returns every node in the project.   | List of node dicts with `operation`, `shape`, `params_json`, …                    |
| `get_dag(project_name, *, pipeline_name=None, run_uid=None, user_tag=None)`   | Convenient “everything I need for a graph”.                          | Same filter rules.                                                                                                                | `{"nodes": [...], "edges": [...]}` where edges come from `transform_edge`.        |
| `get_projects_with_pipelines()`                                               | High-level catalogue: each project, its pipelines and run-counts.    | *(none)*                                                                                                                          | `[ { "project_name": "Proj", "pipelines":[{"name":"etl","runs":7}] }, … ]` |

### Parameter rules at a glance

| Filter combo                   | What you get                              |
| ------------------------------ | ----------------------------------------- |
| `project_name` **only**        | all nodes / all edges in the project      |
| `project_name + pipeline_name` | all runs & nodes for that pipeline        |
| `project_name + run_uid`       | nodes/edges of one specific run           |
| `user_tag`                     | optional extra filter on any of the above |

### Example usage

```python
# 1️⃣ list all runs of “train_model” in “ML_Proj”
runs = wal_writer.get_runs("ML_Proj", pipeline_name="train_model")
first_run = runs[0]["UID"]

# 2️⃣ pull the DAG for that first run
dag = wal_writer.get_dag("ML_Proj", run_uid=first_run)

# 3️⃣ quick print
for n in dag["nodes"]:
    print(n["operation"], n["shape"])
```

> These helpers leverage the official **[Walacor Python SDK](https://github.com/walacor/python-sdk)**, so every call hits Walacor’s fast *summary* view and transparently re-uses the writer’s authenticated session—no extra login or handshake required.

---

## 🤝 Contributing

1. Fork → feature branch → PR.
2. Run `pre-commit run --all-files`.
3. Add/Update unit tests and **schema definitions**.
4. Keep the README & docs in sync.

---

## 📄 License

Apache 2.0 © 2025 Walacor & Contributors.
