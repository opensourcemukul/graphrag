# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing run_workflow method definition."""

import logging
import os

import pandas as pd

from graphrag.config.models.embed_graph_config import EmbedGraphConfig
from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.index.operations.create_graph import create_graph
from graphrag.index.operations.finalize_entities import finalize_entities
from graphrag.index.operations.finalize_relationships import finalize_relationships
from graphrag.index.operations.snapshot_graphml import snapshot_graphml
from graphrag.index.operations.snapshot_neo4j import snapshot_neo4j
from graphrag.index.typing.context import PipelineRunContext
from graphrag.index.typing.workflow import WorkflowFunctionOutput
from graphrag.utils.storage import load_table_from_storage, write_table_to_storage

logger = logging.getLogger(__name__)


async def run_workflow(
    config: GraphRagConfig,
    context: PipelineRunContext,
) -> WorkflowFunctionOutput:
    """All the steps to create the base entity graph."""
    logger.info("Workflow started: finalize_graph")
    entities = await load_table_from_storage("entities", context.output_storage)
    relationships = await load_table_from_storage(
        "relationships", context.output_storage
    )

    final_entities, final_relationships = finalize_graph(
        entities,
        relationships,
        embed_config=config.embed_graph,
        layout_enabled=config.umap.enabled,
    )

    # Only write to Parquet if Neo4j is not the primary backend
    neo4j_only_mode = os.getenv("GRAPHRAG_NEO4J_ONLY", "").lower() in ("1", "true", "yes")
    
    if not neo4j_only_mode:
        await write_table_to_storage(final_entities, "entities", context.output_storage)
        await write_table_to_storage(
            final_relationships, "relationships", context.output_storage
        )
    else:
        logger.info("Neo4j-only mode enabled: skipping Parquet writes for entities and relationships")

    # Optional Neo4j snapshot controlled by environment variables
    # Set GRAPHRAG_NEO4J_ENABLE=true and provide GRAPHRAG_NEO4J_URI, USERNAME, PASSWORD
    if os.getenv("GRAPHRAG_NEO4J_ENABLE", "").lower() in ("1", "true", "yes"):  # type: ignore[call-arg]
        neo4j_uri = os.getenv("GRAPHRAG_NEO4J_URI", "")
        neo4j_user = os.getenv("GRAPHRAG_NEO4J_USERNAME", "")
        neo4j_password = os.getenv("GRAPHRAG_NEO4J_PASSWORD", "")
        neo4j_db = os.getenv("GRAPHRAG_NEO4J_DATABASE")
        if neo4j_uri and neo4j_user and neo4j_password:
            try:
                await snapshot_neo4j(
                    final_entities,
                    final_relationships,
                    uri=neo4j_uri,
                    username=neo4j_user,
                    password=neo4j_password,
                    database=neo4j_db,
                )
            except Exception:
                logger.exception("Error during Neo4j snapshot; continuing without failure")
        else:
            logger.warning(
                "Neo4j snapshot enabled but missing connection envs: GRAPHRAG_NEO4J_URI/USERNAME/PASSWORD"
            )

    if config.snapshots.graphml:
        # todo: extract graphs at each level, and add in meta like descriptions
        graph = create_graph(final_relationships, edge_attr=["weight"])

        await snapshot_graphml(
            graph,
            name="graph",
            storage=context.output_storage,
        )

    logger.info("Workflow completed: finalize_graph")
    return WorkflowFunctionOutput(
        result={
            "entities": entities,
            "relationships": relationships,
        }
    )


def finalize_graph(
    entities: pd.DataFrame,
    relationships: pd.DataFrame,
    embed_config: EmbedGraphConfig | None = None,
    layout_enabled: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """All the steps to finalize the entity and relationship formats."""
    final_entities = finalize_entities(
        entities, relationships, embed_config, layout_enabled
    )
    final_relationships = finalize_relationships(relationships)
    return (final_entities, final_relationships)
