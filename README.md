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

## 🤝 Contributing

1. Fork → feature branch → PR.
2. Run `pre-commit run --all-files`.
3. Add/Update unit tests and **schema definitions**.
4. Keep the README & docs in sync.

---

## 📄 License

Apache 2.0 © 2025 Walacor & Contributors.
