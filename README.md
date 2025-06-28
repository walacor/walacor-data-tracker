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
from walatrack import Tracker, PandasAdapter
from walatrack.writers import ConsoleWriter
from walatrack.writers.walacor import WalacorWriter

# 1. Start the tracker and adapter
tracker = Tracker().start()
adapter = PandasAdapter().start(tracker)

# 2. Define writers (console, or send to Walacor backend)
console_writer = ConsoleWriter()
walacor_writer = WalacorWriter(
    base_url="http://your-walacor-url/api",
    username="your-username",
    password="your-password",
    project_name="MyProject",
    description="Optiona Description"
)

# 3. Apply transformations as usual
df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
df2 = df.assign(new_val=df.value * 2)
df3 = df2.rename(columns={"value": "v"})

# 4. Stop and export the lineage
tracker.stop()

````

> 💡 The `PandasAdapter` automatically tracks operations like `.assign()`, `.rename()`, `.merge()`, etc., so you can work with pandas as usual — but with versioned lineage behind the scenes.


---

This snippet:
- Is short enough to understand at a glance
- Avoids hardcoded credentials or IPs
- Clearly reflects your existing setup
- Shows the power and simplicity of the library


## 🤝 Contributing

1. Fork → feature branch → PR.
2. Run `pre-commit run --all-files`.
3. Add/Update unit tests and **schema definitions**.
4. Keep the README & docs in sync.

---

## 📄 License

Apache 2.0 © 2025 Walacor & Contributors.
