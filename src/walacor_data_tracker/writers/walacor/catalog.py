from __future__ import annotations

from typing import Any, Dict, List, Tuple, cast

from walacor_sdk import WalacorService

from .schema_builder import (
    TRANSFORM_EDGE_ETID,
    TRANSFORM_NODE_ETID,
    TRANSFORM_PROJECT_ETID,
    TRANSFORM_RUN_ETID,
)

JsonRow = Dict[str, Any]
JsonRows = List[JsonRow]


class Catalog:

    def __init__(self, walacor_service: WalacorService) -> None:
        self._dr = walacor_service.data_requests

    def list_projects(self) -> List[JsonRow]:
        rows: JsonRows = self._dr.get_single_record_by_record_id(
            {}, ETId=TRANSFORM_PROJECT_ETID, fromSummary=True
        )
        return [
            {
                "uid": r["UID"],
                "project_name": r["project_name"],
                "description": r.get("description"),
                "user_tag": r.get("user_tag"),
            }
            for r in rows
        ]

    def list_pipelines(self) -> List[str]:
        rows: JsonRows = self._dr.get_single_record_by_record_id(
            {}, ETId=TRANSFORM_RUN_ETID, fromSummary=True
        )
        return sorted({r["pipeline_name"] for r in rows})

    def list_pipelines_for_project(
        self,
        project_name: str,
        *,
        user_tag: str | None = None,
    ) -> List[str]:
        prows: JsonRows = self._dr.get_single_record_by_record_id(
            {
                "project_name": project_name,
                **({"user_tag": user_tag} if user_tag else {}),
            },
            ETId=TRANSFORM_PROJECT_ETID,
        )
        if not prows:
            return []
        proj_uid: str = prows[0]["UID"]

        runs: JsonRows = self._dr.get_single_record_by_record_id(
            {"project_uid": proj_uid},
            ETId=TRANSFORM_RUN_ETID,
            fromSummary=True,
        )
        return sorted({r["pipeline_name"] for r in runs})

    def list_runs(
        self,
        project_name: str,
        *,
        pipeline_name: str | None = None,
        user_tag: str | None = None,
    ) -> JsonRows:
        prows: JsonRows = self._dr.get_single_record_by_record_id(
            {
                "project_name": project_name,
                **({"user_tag": user_tag} if user_tag else {}),
            },
            ETId=TRANSFORM_PROJECT_ETID,
        )
        if not prows:
            return []
        proj_uid: str = prows[0]["UID"]

        flt: Dict[str, Any] = {"project_uid": proj_uid}
        if pipeline_name:
            flt["pipeline_name"] = pipeline_name

        return cast(
            JsonRows,
            self._dr.get_single_record_by_record_id(
                flt, ETId=TRANSFORM_RUN_ETID, fromSummary=True
            ),
        )

    def list_projects_with_pipelines(self) -> List[JsonRow]:
        projects: JsonRows = self._dr.get_single_record_by_record_id(
            {}, ETId=TRANSFORM_PROJECT_ETID, fromSummary=True
        )
        runs: JsonRows = self._dr.get_single_record_by_record_id(
            {}, ETId=TRANSFORM_RUN_ETID, fromSummary=True
        )

        from collections import defaultdict

        run_counter: Dict[Tuple[str, str], int] = defaultdict(int)
        for r in runs:
            run_counter[(r["project_uid"], r["pipeline_name"])] += 1

        out: List[JsonRow] = []
        for p in projects:
            proj_uid: str = p["UID"]
            pipelines = [
                {"name": pl, "runs": run_counter[(proj_uid, pl)]}
                for pl in sorted({pl for (uid, pl) in run_counter if uid == proj_uid})
            ]
            out.append(
                {
                    "project_name": p["project_name"],
                    "user_tag": p.get("user_tag"),
                    "pipelines": pipelines,
                }
            )
        return out

    def list_nodes(
        self,
        *,
        project_name: str,
        pipeline_name: str | None = None,
        run_uid: str | None = None,
        user_tag: str | None = None,
    ) -> JsonRows:
        prows: JsonRows = self._dr.get_single_record_by_record_id(
            {
                "project_name": project_name,
                **({"user_tag": user_tag} if user_tag else {}),
            },
            ETId=TRANSFORM_PROJECT_ETID,
        )
        if not prows:
            return []
        proj_uid: str = prows[0]["UID"]

        flt: Dict[str, Any] = {"project_uid": proj_uid}

        if run_uid:
            flt["run_uid"] = run_uid
        elif pipeline_name:
            run_rows: JsonRows = self._dr.get_single_record_by_record_id(
                {"project_uid": proj_uid, "pipeline_name": pipeline_name},
                ETId=TRANSFORM_RUN_ETID,
                fromSummary=True,
            )
            if not run_rows:
                return []
            flt["run_uid"] = {"$in": [r["UID"] for r in run_rows]}

        return cast(
            JsonRows,
            self._dr.get_single_record_by_record_id(
                flt, ETId=TRANSFORM_NODE_ETID, fromSummary=True
            ),
        )

    def list_dag(
        self,
        *,
        project_name: str,
        pipeline_name: str | None = None,
        run_uid: str | None = None,
        user_tag: str | None = None,
    ) -> Dict[str, JsonRows]:
        nodes: JsonRows = self.list_nodes(
            project_name=project_name,
            pipeline_name=pipeline_name,
            run_uid=run_uid,
            user_tag=user_tag,
        )
        if not nodes:
            return {"nodes": [], "edges": []}

        node_uids = [n["UID"] for n in nodes]
        edges: JsonRows = self._dr.get_single_record_by_record_id(
            {
                "$or": [
                    {"parent_node_uid": {"$in": node_uids}},
                    {"child_node_uid": {"$in": node_uids}},
                ]
            },
            ETId=TRANSFORM_EDGE_ETID,
            fromSummary=True,
        )
        return {"nodes": nodes, "edges": edges}
