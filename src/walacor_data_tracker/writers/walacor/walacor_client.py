from __future__ import annotations

import json
import logging

from typing import Any, Dict, List, cast

from walacor_sdk import WalacorService
from walacor_sdk.file_request import DuplicateData, FileInfo, StoreFileData

from walacor_data_tracker.core.snapshot import Snapshot
from walacor_data_tracker.writers.walacor._json import jsonify

from .catalog import Catalog
from .schema_builder import (
    TRANSFORM_EDGE_ETID,
    TRANSFORM_NODE_ETID,
    TRANSFORM_PROJECT_ETID,
    TRANSFORM_RUN_ETID,
    SchemaBuilder,
)

_LOG = logging.getLogger(__name__)
JsonRow = Dict[str, Any]
JsonRows = List[JsonRow]


class WalacorClient:

    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        *,
        project_name: str,
        description: str | None = None,
        user_tag: str | None = None,
    ) -> None:
        self._walacor = WalacorService(
            server=server, username=username, password=password
        )
        self.catalog = Catalog(self._walacor)

        self._ensure_schema()
        self._project_uid: str = self._ensure_project_row(
            project_name=project_name, description=description, user_tag=user_tag
        )

        self._last_node_uid: str | None = None
        self._run_uid: str | None = None

    def insert_row(
        self,
        snapshot: Snapshot,
        *,
        parent_uid: str | None = None,
        run_uid: str | None = None,
    ) -> str:
        run_uid = run_uid or self._run_uid
        if run_uid is None:
            raise RuntimeError("Run not started – call WalacorWriter.begin_run()")

        file_info: FileInfo | DuplicateData = (
            self._walacor.file_request.verify_in_memory(snapshot.artifact)
        )
        artefact_uid: str
        if isinstance(file_info, FileInfo):
            stored: StoreFileData = self._walacor.file_request.store(
                file_info=file_info
            )
            artefact_uid = stored.UID[0]
        else:
            artefact_uid = file_info.uid[0]

        node_row: JsonRow = {
            "project_uid": self._project_uid,
            "run_uid": run_uid,
            "artifact_uid": artefact_uid,
            "operation": snapshot.operation,
            "shape": list(snapshot.shape),
            "params_json": json.dumps(jsonify(snapshot.kwargs)),
        }
        res = self._walacor.data_requests.insert_single_record(
            node_row, TRANSFORM_NODE_ETID
        )
        if res is None or not res.UID:
            raise RuntimeError("Failed to insert transform_node row")

        node_uid: str = cast(str, res.UID[0])

        parent = parent_uid or self._last_node_uid
        if parent:
            self._walacor.data_requests.insert_single_record(
                {"parent_node_uid": parent, "child_node_uid": node_uid},
                TRANSFORM_EDGE_ETID,
            )
        self._last_node_uid = node_uid
        return node_uid

    def ensure_run_row(self, *, pipeline_name: str, status: str = "running") -> str:
        lookup = {
            "project_uid": self._project_uid,
            "pipeline_name": pipeline_name,
            "status": status,
        }
        existing = self._walacor.data_requests.get_single_record_by_record_id(
            lookup, ETId=TRANSFORM_RUN_ETID, fromSummary=True
        )
        if existing:
            self._run_uid = cast(str, existing[0]["UID"])
            return self._run_uid

        res = self._walacor.data_requests.insert_single_record(
            lookup, TRANSFORM_RUN_ETID
        )
        if res is None or not res.UID:
            raise RuntimeError("Failed to insert transform_run row")
        self._run_uid = cast(str, res.UID[0])
        return self._run_uid

    def update_run_status(self, status: str) -> None:
        if self._run_uid:
            self._walacor.data_requests.update_single_record_with_UID(
                {"UID": self._run_uid, "status": status},
                TRANSFORM_RUN_ETID,
            )

    def _ensure_schema(self) -> None:
        check = self._walacor.schema.get_schema_details_with_ETId
        create = self._walacor.schema.create_schema
        for etid, factory in [
            (TRANSFORM_PROJECT_ETID, SchemaBuilder.project),
            (TRANSFORM_NODE_ETID, SchemaBuilder.node),
            (TRANSFORM_EDGE_ETID, SchemaBuilder.edge),
            (TRANSFORM_RUN_ETID, SchemaBuilder.run),
        ]:
            try:
                check(etid)
            except Exception:
                _LOG.info("Creating Walacor schema ETId %s", etid)
                create(factory())

    def _ensure_project_row(
        self,
        *,
        project_name: str,
        description: str | None,
        user_tag: str | None,
    ) -> str:
        rows = self._walacor.data_requests.get_single_record_by_record_id(
            {"project_name": project_name}, ETId=TRANSFORM_PROJECT_ETID
        )
        if rows:
            return cast(str, rows[0]["UID"])

        row = {
            "project_name": project_name,
            "description": description or "",
            "user_tag": user_tag or "",
        }
        res = self._walacor.data_requests.insert_single_record(
            row, TRANSFORM_PROJECT_ETID
        )
        if res is None or not res.UID:
            raise RuntimeError("Could not create project_metadata row")
        return cast(str, res.UID[0])

    def list_projects(self) -> List[JsonRow]:
        return self.catalog.list_projects()

    def list_pipelines(self) -> List[str]:
        return self.catalog.list_pipelines()

    def list_pipelines_for_project(self, *args: Any, **kw: Any) -> List[str]:
        return self.catalog.list_pipelines_for_project(*args, **kw)

    def list_runs(self, *args: Any, **kw: Any) -> JsonRows:
        return self.catalog.list_runs(*args, **kw)

    def list_nodes(self, *args: Any, **kw: Any) -> JsonRows:
        return self.catalog.list_nodes(*args, **kw)

    def list_dag(self, project_name: str, **kw: Any) -> Dict[str, JsonRows]:
        return self.catalog.list_dag(project_name=project_name, **kw)

    def list_projects_with_pipelines(self) -> List[JsonRow]:
        return self.catalog.list_projects_with_pipelines()
