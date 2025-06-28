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

# ---------- constants ----------------------------------------------------
TRANSFORM_PROJECT_ETID = 20000
TRANSFORM_NODE_ETID = 20001
TRANSFORM_EDGE_ETID = 20002

TRANSFORM_PROJECT_TABLE_NAME = "project_metadata"
TRANSFORM_NODE_TABLE_NAME = "transform_node"
TRANSFORM_EDGE_TABLE_NAME = "transform_edge"

TRANSFORM_FAMILY = "DataScience"


# ========================================================================
class WalacorClient:
    # ---------------- ctor ----------------------------------------------
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

        self._ensure_schema()

        self._project_uid: str = self._ensure_project_row(
            project_name=project_name,
            description=description,
            user_tag=user_tag,
        )

        self._last_node_uid: str | None = None

    # ---------------- public API -----------------------------------------
    def insert_row(
        self,
        snapshot: Snapshot,
        *,
        parent_uid: str | None = None,
    ) -> str:
        """
        Upload artefact ➜ insert node ➜ insert edge (optional).
        Returns Walacor-generated node UID.
        """
        # ---- 1. artefact upload / dedup ---------------------------------
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
            "artifact_uid": artefact_uid,
            "operation": snapshot.operation,
            "shape": list(snapshot.shape),
            "params_json": json.dumps(jsonify(snapshot.kwargs)),
        }

        node_res: SubmissionResult | None = (
            self._walacor.data_requests.insert_single_record(
                node_row,
                TRANSFORM_NODE_ETID,
            )
        )

        if node_res is None or not node_res.UID:
            raise RuntimeError("Failed to insert transform_node row")

        assert isinstance(node_res.UID[0], str)
        node_uid: str = node_res.UID[0]

        # ---- 3. insert EDGE (if parent known) ---------------------------
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

        # ---- 4. chain bookkeeping --------------------------------------
        self._last_node_uid = node_uid
        return node_uid

    # ==================================================================== helpers
    def _ensure_schema(self) -> None:
        schemas = self._build_schema_requests()
        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_PROJECT_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor TRANSFORM_PROJECT schema")
            self._walacor.schema.create_schema(schemas[0])

        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_NODE_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor transform schema")
            self._walacor.schema.create_schema(schemas[1])

        try:
            self._walacor.schema.get_schema_details_with_ETId(TRANSFORM_EDGE_ETID)
        except Exception:  # pragma: no cover
            _LOG.info("Creating Walacor transform schema")
            self._walacor.schema.create_schema(schemas[2])

    def _build_schema_requests(self) -> list[CreateSchemaRequest]:
        return [
            CreateSchemaRequest(
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
            ),
            CreateSchemaRequest(
                Schema=CreateSchemaDefinition(
                    ETId=TRANSFORM_NODE_ETID,
                    TableName=TRANSFORM_NODE_TABLE_NAME,
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
                            FieldName="shape", DataType=FieldType.ARRAY, Required=False
                        ),
                        CreateFieldRequest(
                            FieldName="params_json",
                            DataType=FieldType.TEXT,
                            Required=False,
                        ),
                    ],
                    Indexes=[
                        CreateIndexRequest(
                            Fields=["project_uid"], IndexValue="project_uid"
                        ),
                    ],
                ),
            ),
            # ---------- transform_edge (ETId 20002)
            CreateSchemaRequest(
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
                ),
            ),
        ]

    def _ensure_project_row(
        self,
        *,
        project_name: str,
        description: str | None,
        user_tag: str | None,
    ) -> str:
        # ---- lookup by name --------------------------------------------
        existing = self._walacor.data_requests.get_single_record_by_record_id(
            {"project_name": project_name},
            ETId=TRANSFORM_PROJECT_ETID,
        )
        if existing:
            uid = existing[0]["UID"]
            assert isinstance(uid, str)
            return uid

        # ---- not found → insert (no UID field) -------------------------
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

        assert isinstance(result.UID[0], str)
        return result.UID[0]  # Walacor-minted
