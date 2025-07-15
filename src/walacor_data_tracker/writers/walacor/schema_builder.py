"""Pure factory that owns *all* Walacor schema definitions."""

from walacor_sdk.schema import (
    CreateFieldRequest,
    CreateIndexRequest,
    CreateSchemaDefinition,
    CreateSchemaRequest,
)
from walacor_sdk.utils.enums import FieldType

TRANSFORM_PROJECT_ETID = 20000
TRANSFORM_NODE_ETID = 20001
TRANSFORM_EDGE_ETID = 20002
TRANSFORM_RUN_ETID = 20003

TRANSFORM_PROJECT_TABLE_NAME = "project_metadata"
TRANSFORM_NODE_TABLE_NAME = "transform_node"
TRANSFORM_EDGE_TABLE_NAME = "transform_edge"
TRANSFORM_RUN_TABLE_NAME = "transform_run"

TRANSFORM_FAMILY = "DataScience"


class SchemaBuilder:
    """Static helpers that emit `CreateSchemaRequest` objects."""

    @staticmethod
    def project() -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_PROJECT_ETID,
                TableName=TRANSFORM_PROJECT_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest("project_name", FieldType.TEXT, True, 50),
                    CreateFieldRequest("description", FieldType.TEXT, False, 500),
                    CreateFieldRequest("user_tag", FieldType.TEXT, False, 50),
                ],
            )
        )

    @staticmethod
    def node() -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_NODE_ETID,
                TableName=TRANSFORM_NODE_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest("run_uid", FieldType.TEXT, True, 50),
                    CreateFieldRequest("project_uid", FieldType.TEXT, True, 50),
                    CreateFieldRequest("artifact_uid", FieldType.TEXT, False, 50),
                    CreateFieldRequest("operation", FieldType.TEXT, True, 50),
                    CreateFieldRequest("shape", FieldType.ARRAY, False),
                    CreateFieldRequest("params_json", FieldType.TEXT, False),
                ],
                Indexes=[
                    CreateIndexRequest(Fields=["project_uid"], IndexValue="project_uid")
                ],
            )
        )

    @staticmethod
    def edge() -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_EDGE_ETID,
                TableName=TRANSFORM_EDGE_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest("parent_node_uid", FieldType.TEXT, True, 50),
                    CreateFieldRequest("child_node_uid", FieldType.TEXT, True, 50),
                ],
                Indexes=[
                    CreateIndexRequest(
                        Fields=["parent_node_uid", "child_node_uid"],
                        IndexValue="parent_child",
                    )
                ],
            )
        )

    @staticmethod
    def run() -> CreateSchemaRequest:
        return CreateSchemaRequest(
            Schema=CreateSchemaDefinition(
                ETId=TRANSFORM_RUN_ETID,
                TableName=TRANSFORM_RUN_TABLE_NAME,
                Family=TRANSFORM_FAMILY,
                DoSummary=True,
                Fields=[
                    CreateFieldRequest("project_uid", FieldType.TEXT, True, 50),
                    CreateFieldRequest("pipeline_name", FieldType.TEXT, True, 50),
                    CreateFieldRequest("status", FieldType.TEXT, True, 20),
                ],
                Indexes=[
                    CreateIndexRequest(
                        Fields=["project_uid"], IndexValue="project_uid"
                    ),
                    CreateIndexRequest(
                        Fields=["pipeline_name"], IndexValue="pipeline_name"
                    ),
                ],
            )
        )
