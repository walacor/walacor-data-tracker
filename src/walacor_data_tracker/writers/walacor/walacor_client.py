from __future__ import annotations

import json
import logging

from walacor_sdk import WalacorService
from walacor_sdk.data_requests import SubmissionResult
from walacor_sdk.file_request import DuplicateData, FileInfo, StoreFileData
from walacor_sdk.schema import (
    CreateFieldRequest,
    CreateIndexRequest,
    CreateSchemaDefinition,
    CreateSchemaRequest,
)
from walacor_sdk.utils.enums import FieldType

from walacor_data_tracker.core.snapshot import Snapshot
from walacor_data_tracker.writers.walacor._json import jsonify

_LOG = logging.getLogger(__name__)

TRANSFORM_PROJECT_ETID = 20000
TRANSFORM_NODE_ETID = 20001
TRANSFORM_EDGE_ETID = 20002
TRANSFORM_RUN_ETID = 20003

TRANSFORM_PROJECT_TABLE_NAME = "project_metadata"
TRANSFORM_NODE_TABLE_NAME = "transform_node"
TRANSFORM_EDGE_TABLE_NAME = "transform_edge"
TRANSFORM_RUN_TABLE_NAME = "transform_run"

TRANSFORM_FAMILY = "DataScience"


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
            server=server,
            username=username,
            password=password,
        )

        self._ensure_schema()

        self._project_uid: str = self._ensure_project_row(
            project_name=project_name,
            description=description,
            user_tag=user_tag,
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
        """
        Upload artefact → insert node → insert edge (optional).
        Returns Walacor-generated node UID.
        """
        run_uid = run_uid or self._run_uid
        if run_uid is None:
            raise RuntimeError(
                "Walacor run not started – call WalacorWriter.begin_run() first"
            )

        file_info: FileInfo | DuplicateData = (
            self._walacor.file_request.verify_in_memory(snapshot.artifact)
        )

        if isinstance(file_info, FileInfo):
            stored: StoreFileData = self._walacor.file_request.store(
                file_info=file_info
            )
            artefact_uid = stored.UID[0]
        else:
            artefact_uid = file_info.uid[0]

        node_row = {
            "project_uid": self._project_uid,
            "run_uid": run_uid,
            "artifact_uid": artefact_uid,
            "operation": snapshot.operation,
            "shape": list(snapshot.shape),
            "params_json": json.dumps(jsonify(snapshot.kwargs)),
        }

        node_res: SubmissionResult | None = self._walacor.data_requests.insert_single_record(
            node_row,
            TRANSFORM_NODE_ETID,
        )

        if node_res is None or not node_res.UID:
            raise RuntimeError("Failed to insert transform_node row")

        node_uid: str = str(node_res.UID[0])

        parent = parent_uid or self._last_node_uid
        if parent:
            edge_row = {"parent_node_uid": parent, "child_node_uid": node_uid}
            edge_res = self._walacor.data_requests.insert_single_record(
                edge_row,
                TRANSFORM_EDGE_ETID,
            )
            if edge_res is None:
                _LOG.error(
                    "Edge creation failed (parent=%s child=%s)", parent, node_uid
                )
        else:
            _LOG.debug("First node in lineage branch – no parent edge written")

        self._last_node_uid = node_uid
        return node_uid

    def ensure_run_row(
        self,
        *,
        pipeline_name: str,
        status: str = "running",
    ) -> str:
        """
        Get or create a `transform_run` row and return its UID.
        Stores the UID on the client for later node inserts.
        """
        lookup = {
            "project_uid": self._project_uid,
            "pipeline_name": pipeline_name,
            "status": status,
        }
        existing = self._walacor.data_requests.get_single_record_by_record_id(
            lookup,
            ETId=TRANSFORM_RUN_ETID,
        )
        if existing:
            uid = str(existing[0]["UID"])
            self._run_uid = uid
            return uid

        res = self._walacor.data_requests.insert_single_record(
            lookup, TRANSFORM_RUN_ETID
        )
        if res is None or not res.UID:
            raise RuntimeError("Failed to insert transform_run row")

        self._run_uid = str(res.UID[0])
        return self._run_uid

    def update_run_status(self, status: str) -> None:
        """
        Write a new *status* into the current transform_run row, adapting to
        whichever update-method the installed Walacor SDK exposes.

        • modern SDK  → update_record_by_uid(uid, patch, ETId)
        • older SDK   → update_single_record_with_UID(record, ETId)
                        where *record* must contain its own UID field.
        """
        if self._run_uid is None:
            return
        #TODO carefull checking
        try: 
            self._walacor.data_requests.update_single_record_with_UID(
                self._run_uid,
                {"status": status},
                TRANSFORM_RUN_ETID,
            )
            return
        except AttributeError:
            pass 

        record = {"UID": self._run_uid, "status": status}
        self._walacor.data_requests.update_single_record_with_UID(
            record, TRANSFORM_RUN_ETID
        )

    def _ensure_schema(self) -> None:
        """Create the four Walacor schemas if they don’t already exist."""
        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_PROJECT_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor project_metadata schema")
            self._walacor.schema.create_schema(self._build_transform_project_schema())

        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_NODE_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor transform_node schema")
            self._walacor.schema.create_schema(self._build_transform_node_schema())

        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_EDGE_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor transform_edge schema")
            self._walacor.schema.create_schema(self._build_transform_edge_schema())

        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_RUN_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor transform_run schema")
            self._walacor.schema.create_schema(self._build_transform_run_schema())

    def _build_transform_project_schema(self) -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_PROJECT_ETID,
                TableName=TRANSFORM_PROJECT_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest(
                        FieldName="project_name",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="description",
                        DataType=FieldType.TEXT,
                        Required=False,
                        MaxLength=500,
                    ),
                    CreateFieldRequest(
                        FieldName="user_tag",
                        DataType=FieldType.TEXT,
                        Required=False,
                        MaxLength=50,
                    ),
                ],
            )
        )

    def _build_transform_node_schema(self) -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_NODE_ETID,
                TableName=TRANSFORM_NODE_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest(
                        FieldName="run_uid",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="project_uid",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="artifact_uid",
                        DataType=FieldType.TEXT,
                        Required=False,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="operation",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="shape",
                        DataType=FieldType.ARRAY,
                        Required=False,
                    ),
                    CreateFieldRequest(
                        FieldName="params_json",
                        DataType=FieldType.TEXT,
                        Required=False,
                    ),
                ],
                Indexes=[
                    CreateIndexRequest(
                        Fields=["project_uid"],
                        IndexValue="project_uid",
                    ),
                ],
            )
        )

    def _build_transform_edge_schema(self) -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_EDGE_ETID,
                TableName=TRANSFORM_EDGE_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest(
                        FieldName="parent_node_uid",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="child_node_uid",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                ],
                Indexes=[
                    CreateIndexRequest(
                        Fields=["parent_node_uid", "child_node_uid"],
                        IndexValue="parent_child",
                    ),
                ],
            )
        )

    def _build_transform_run_schema(self) -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_RUN_ETID,
                TableName=TRANSFORM_RUN_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest(
                        FieldName="project_uid",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="pipeline_name",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=50,
                    ),
                    CreateFieldRequest(
                        FieldName="status",
                        DataType=FieldType.TEXT,
                        Required=True,
                        MaxLength=20,
                    ),
                ],
                Indexes=[
                    CreateIndexRequest(
                        Fields=["project_uid"],
                        IndexValue="project_uid",
                    ),
                    CreateIndexRequest(
                        Fields=["pipeline_name"],
                        IndexValue="pipeline_name",
                    ),
                ],
            )
        )

    def _ensure_project_row(
        self,
        *,
        project_name: str,
        description: str | None,
        user_tag: str | None,
    ) -> str:
        # lookup by name
        existing = self._walacor.data_requests.get_single_record_by_record_id(
            {"project_name": project_name},
            ETId=TRANSFORM_PROJECT_ETID,
        )
        if existing:
            return str(existing[0]["UID"])

        # insert
        project_row = {
            "project_name": project_name,
            "description": description or "",
            "user_tag": user_tag or "",
        }
        result = self._walacor.data_requests.insert_single_record(
            project_row,
            TRANSFORM_PROJECT_ETID,
        )
        if result is None or not result.UID:
            raise RuntimeError("Could not create project_metadata row")

        return str(result.UID[0])
