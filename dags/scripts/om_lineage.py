import os
from functools import lru_cache
from typing import Optional

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata

STORAGE_SERVICE_NAME = os.getenv("OPENMETADATA_STORAGE_SERVICE", "minio")


@lru_cache(maxsize=1)
def get_ometa_client() -> OpenMetadata:
    connection = OpenMetadataConnection(
        hostPort=os.getenv("OPENMETADATA_HOST_PORT", "http://openmetadata-server:8585/api"),
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=os.getenv("OPENMETADATA_JWT_TOKEN")),
    )
    return OpenMetadata(connection)


def link_buckets(bucket_from: str, bucket_to: str, pipeline_fqn: Optional[str] = None) -> None:
    """Record lineage between two MinIO buckets (Container entities) in OpenMetadata,
    tagged with the Airflow pipeline that performed the transformation."""
    ometa = get_ometa_client()

    from_ref = ometa.get_entity_reference(
        entity=Container, fqn=f"{STORAGE_SERVICE_NAME}.{bucket_from}"
    )
    to_ref = ometa.get_entity_reference(
        entity=Container, fqn=f"{STORAGE_SERVICE_NAME}.{bucket_to}"
    )
    if not from_ref or not to_ref:
        print(f"[lineage] Container(s) not found in OpenMetadata: {bucket_from} -> {bucket_to}")
        return

    lineage_details = None
    if pipeline_fqn:
        pipeline_ref = ometa.get_entity_reference(entity=Pipeline, fqn=pipeline_fqn)
        if pipeline_ref:
            lineage_details = LineageDetails(pipeline=pipeline_ref)

    request = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=from_ref,
            toEntity=to_ref,
            lineageDetails=lineage_details,
        )
    )
    ometa.add_lineage(request)
    print(f"[lineage] {bucket_from} -> {bucket_to}" + (f" via {pipeline_fqn}" if pipeline_fqn else ""))
