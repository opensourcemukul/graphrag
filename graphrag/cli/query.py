# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""CLI implementation of the query subcommand."""

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

import graphrag.api as api
from graphrag.callbacks.noop_query_callbacks import NoopQueryCallbacks
from graphrag.config.load_config import load_config
from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.utils.api import create_storage_from_config
from graphrag.utils.storage import load_table_from_storage, storage_has_table

if TYPE_CHECKING:
    import pandas as pd

# ruff: noqa: T201

logger = logging.getLogger(__name__)


def run_global_search(
    config_filepath: Path | None,
    data_dir: Path | None,
    root_dir: Path,
    community_level: int | None,
    dynamic_community_selection: bool,
    response_type: str,
    streaming: bool,
    query: str,
    verbose: bool,
):
    """Perform a global search with a given query.

    Loads index files required for global search and calls the Query API.
    """
    root = root_dir.resolve()
    cli_overrides = {}
    if data_dir:
        cli_overrides["output.base_dir"] = str(data_dir)
    config = load_config(root, config_filepath, cli_overrides)

    dataframe_dict = _resolve_output_files(
        config=config,
        output_list=[
            "entities",
            "communities",
            "community_reports",
        ],
        optional_list=[],
    )

    # Call the Multi-Index Global Search API
    if dataframe_dict["multi-index"]:
        final_entities_list = dataframe_dict["entities"]
        final_communities_list = dataframe_dict["communities"]
        final_community_reports_list = dataframe_dict["community_reports"]
        index_names = dataframe_dict["index_names"]

        response, context_data = asyncio.run(
            api.multi_index_global_search(
                config=config,
                entities_list=final_entities_list,
                communities_list=final_communities_list,
                community_reports_list=final_community_reports_list,
                index_names=index_names,
                community_level=community_level,
                dynamic_community_selection=dynamic_community_selection,
                response_type=response_type,
                streaming=streaming,
                query=query,
                verbose=verbose,
            )
        )
        print(response)
        return response, context_data

    # Otherwise, call the Single-Index Global Search API
    final_entities: pd.DataFrame = dataframe_dict["entities"]
    final_communities: pd.DataFrame = dataframe_dict["communities"]
    final_community_reports: pd.DataFrame = dataframe_dict["community_reports"]

    if streaming:

        async def run_streaming_search():
            full_response = ""
            context_data = {}

            def on_context(context: Any) -> None:
                nonlocal context_data
                context_data = context

            callbacks = NoopQueryCallbacks()
            callbacks.on_context = on_context

            async for stream_chunk in api.global_search_streaming(
                config=config,
                entities=final_entities,
                communities=final_communities,
                community_reports=final_community_reports,
                community_level=community_level,
                dynamic_community_selection=dynamic_community_selection,
                response_type=response_type,
                query=query,
                callbacks=[callbacks],
                verbose=verbose,
            ):
                full_response += stream_chunk
                print(stream_chunk, end="")
                sys.stdout.flush()
            print()
            return full_response, context_data

        return asyncio.run(run_streaming_search())
    # not streaming
    response, context_data = asyncio.run(
        api.global_search(
            config=config,
            entities=final_entities,
            communities=final_communities,
            community_reports=final_community_reports,
            community_level=community_level,
            dynamic_community_selection=dynamic_community_selection,
            response_type=response_type,
            query=query,
            verbose=verbose,
        )
    )
    print(response)

    return response, context_data


def run_local_search(
    config_filepath: Path | None,
    data_dir: Path | None,
    root_dir: Path,
    community_level: int,
    response_type: str,
    streaming: bool,
    query: str,
    verbose: bool,
):
    """Perform a local search with a given query.

    Loads index files required for local search and calls the Query API.
    """
    root = root_dir.resolve()
    cli_overrides = {}
    if data_dir:
        cli_overrides["output.base_dir"] = str(data_dir)
    config = load_config(root, config_filepath, cli_overrides)

    dataframe_dict = _resolve_output_files(
        config=config,
        output_list=[
            "communities",
            "community_reports",
            "text_units",
            "relationships",
            "entities",
        ],
        optional_list=[
            "covariates",
        ],
    )
    # Call the Multi-Index Local Search API
    if dataframe_dict["multi-index"]:
        final_entities_list = dataframe_dict["entities"]
        final_communities_list = dataframe_dict["communities"]
        final_community_reports_list = dataframe_dict["community_reports"]
        final_text_units_list = dataframe_dict["text_units"]
        final_relationships_list = dataframe_dict["relationships"]
        index_names = dataframe_dict["index_names"]

        # If any covariates tables are missing from any index, set the covariates list to None
        if len(dataframe_dict["covariates"]) != dataframe_dict["num_indexes"]:
            final_covariates_list = None
        else:
            final_covariates_list = dataframe_dict["covariates"]

        response, context_data = asyncio.run(
            api.multi_index_local_search(
                config=config,
                entities_list=final_entities_list,
                communities_list=final_communities_list,
                community_reports_list=final_community_reports_list,
                text_units_list=final_text_units_list,
                relationships_list=final_relationships_list,
                covariates_list=final_covariates_list,
                index_names=index_names,
                community_level=community_level,
                response_type=response_type,
                streaming=streaming,
                query=query,
                verbose=verbose,
            )
        )
        print(response)

        return response, context_data

    # Otherwise, call the Single-Index Local Search API
    final_communities: pd.DataFrame = dataframe_dict["communities"]
    final_community_reports: pd.DataFrame = dataframe_dict["community_reports"]
    final_text_units: pd.DataFrame = dataframe_dict["text_units"]
    final_relationships: pd.DataFrame = dataframe_dict["relationships"]
    final_entities: pd.DataFrame = dataframe_dict["entities"]
    final_covariates: pd.DataFrame | None = dataframe_dict["covariates"]

    if streaming:

        async def run_streaming_search():
            full_response = ""
            context_data = {}

            def on_context(context: Any) -> None:
                nonlocal context_data
                context_data = context

            callbacks = NoopQueryCallbacks()
            callbacks.on_context = on_context

            async for stream_chunk in api.local_search_streaming(
                config=config,
                entities=final_entities,
                communities=final_communities,
                community_reports=final_community_reports,
                text_units=final_text_units,
                relationships=final_relationships,
                covariates=final_covariates,
                community_level=community_level,
                response_type=response_type,
                query=query,
                callbacks=[callbacks],
                verbose=verbose,
            ):
                full_response += stream_chunk
                print(stream_chunk, end="")
                sys.stdout.flush()
            print()
            return full_response, context_data

        return asyncio.run(run_streaming_search())
    # not streaming
    response, context_data = asyncio.run(
        api.local_search(
            config=config,
            entities=final_entities,
            communities=final_communities,
            community_reports=final_community_reports,
            text_units=final_text_units,
            relationships=final_relationships,
            covariates=final_covariates,
            community_level=community_level,
            response_type=response_type,
            query=query,
            verbose=verbose,
        )
    )
    print(response)

    return response, context_data


def run_drift_search(
    config_filepath: Path | None,
    data_dir: Path | None,
    root_dir: Path,
    community_level: int,
    response_type: str,
    streaming: bool,
    query: str,
    verbose: bool,
):
    """Perform a local search with a given query.

    Loads index files required for local search and calls the Query API.
    """
    root = root_dir.resolve()
    cli_overrides = {}
    if data_dir:
        cli_overrides["output.base_dir"] = str(data_dir)
    config = load_config(root, config_filepath, cli_overrides)

    dataframe_dict = _resolve_output_files(
        config=config,
        output_list=[
            "communities",
            "community_reports",
            "text_units",
            "relationships",
            "entities",
        ],
    )

    # Call the Multi-Index Drift Search API
    if dataframe_dict["multi-index"]:
        final_entities_list = dataframe_dict["entities"]
        final_communities_list = dataframe_dict["communities"]
        final_community_reports_list = dataframe_dict["community_reports"]
        final_text_units_list = dataframe_dict["text_units"]
        final_relationships_list = dataframe_dict["relationships"]
        index_names = dataframe_dict["index_names"]

        response, context_data = asyncio.run(
            api.multi_index_drift_search(
                config=config,
                entities_list=final_entities_list,
                communities_list=final_communities_list,
                community_reports_list=final_community_reports_list,
                text_units_list=final_text_units_list,
                relationships_list=final_relationships_list,
                index_names=index_names,
                community_level=community_level,
                response_type=response_type,
                streaming=streaming,
                query=query,
                verbose=verbose,
            )
        )
        print(response)

        return response, context_data

    # Otherwise, call the Single-Index Drift Search API
    final_communities: pd.DataFrame = dataframe_dict["communities"]
    final_community_reports: pd.DataFrame = dataframe_dict["community_reports"]
    final_text_units: pd.DataFrame = dataframe_dict["text_units"]
    final_relationships: pd.DataFrame = dataframe_dict["relationships"]
    final_entities: pd.DataFrame = dataframe_dict["entities"]

    if streaming:

        async def run_streaming_search():
            full_response = ""
            context_data = {}

            def on_context(context: Any) -> None:
                nonlocal context_data
                context_data = context

            callbacks = NoopQueryCallbacks()
            callbacks.on_context = on_context

            async for stream_chunk in api.drift_search_streaming(
                config=config,
                entities=final_entities,
                communities=final_communities,
                community_reports=final_community_reports,
                text_units=final_text_units,
                relationships=final_relationships,
                community_level=community_level,
                response_type=response_type,
                query=query,
                callbacks=[callbacks],
                verbose=verbose,
            ):
                full_response += stream_chunk
                print(stream_chunk, end="")
                sys.stdout.flush()
            print()
            return full_response, context_data

        return asyncio.run(run_streaming_search())

    # not streaming
    response, context_data = asyncio.run(
        api.drift_search(
            config=config,
            entities=final_entities,
            communities=final_communities,
            community_reports=final_community_reports,
            text_units=final_text_units,
            relationships=final_relationships,
            community_level=community_level,
            response_type=response_type,
            query=query,
            verbose=verbose,
        )
    )
    print(response)

    return response, context_data


def run_basic_search(
    config_filepath: Path | None,
    data_dir: Path | None,
    root_dir: Path,
    streaming: bool,
    query: str,
    verbose: bool,
):
    """Perform a basics search with a given query.

    Loads index files required for basic search and calls the Query API.
    """
    root = root_dir.resolve()
    cli_overrides = {}
    if data_dir:
        cli_overrides["output.base_dir"] = str(data_dir)
    config = load_config(root, config_filepath, cli_overrides)

    dataframe_dict = _resolve_output_files(
        config=config,
        output_list=[
            "text_units",
        ],
    )

    # Call the Multi-Index Basic Search API
    if dataframe_dict["multi-index"]:
        final_text_units_list = dataframe_dict["text_units"]
        index_names = dataframe_dict["index_names"]

        response, context_data = asyncio.run(
            api.multi_index_basic_search(
                config=config,
                text_units_list=final_text_units_list,
                index_names=index_names,
                streaming=streaming,
                query=query,
                verbose=verbose,
            )
        )
        print(response)

        return response, context_data

    # Otherwise, call the Single-Index Basic Search API
    final_text_units: pd.DataFrame = dataframe_dict["text_units"]

    if streaming:

        async def run_streaming_search():
            full_response = ""
            context_data = {}

            def on_context(context: Any) -> None:
                nonlocal context_data
                context_data = context

            callbacks = NoopQueryCallbacks()
            callbacks.on_context = on_context

            async for stream_chunk in api.basic_search_streaming(
                config=config,
                text_units=final_text_units,
                query=query,
                callbacks=[callbacks],
                verbose=verbose,
            ):
                full_response += stream_chunk
                print(stream_chunk, end="")
                sys.stdout.flush()
            print()
            return full_response, context_data

        return asyncio.run(run_streaming_search())
    # not streaming
    response, context_data = asyncio.run(
        api.basic_search(
            config=config,
            text_units=final_text_units,
            query=query,
            verbose=verbose,
        )
    )
    print(response)

    return response, context_data


def _load_entities_relationships_from_neo4j() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Optionally load entities and relationships from Neo4j if configured.

    Controlled by env var GRAPHRAG_QUERY_BACKEND=neo4j and connection envs:
      GRAPHRAG_NEO4J_URI, GRAPHRAG_NEO4J_USERNAME, GRAPHRAG_NEO4J_PASSWORD, optional GRAPHRAG_NEO4J_DATABASE

    Returns a (entities_df, relationships_df) tuple or None if not enabled/available.
    """
    if os.getenv("GRAPHRAG_QUERY_BACKEND", "").lower() != "neo4j":
        return None
    try:
        from neo4j import GraphDatabase  # type: ignore
        import pandas as pd  # type: ignore
    except Exception:
        # Driver not available; fall back silently
        return None

    uri = os.getenv("GRAPHRAG_NEO4J_URI", "")
    user = os.getenv("GRAPHRAG_NEO4J_USERNAME", "")
    pwd = os.getenv("GRAPHRAG_NEO4J_PASSWORD", "")
    db = os.getenv("GRAPHRAG_NEO4J_DATABASE")
    if not (uri and user and pwd):
        return None

    driver = GraphDatabase.driver(uri, auth=(user, pwd))

    # Get all properties from nodes and relationships
    node_query = "MATCH (n:__Entity__) RETURN n"
    rel_query = (
        "MATCH (s:__Entity__)-[r:RELATED]->(t:__Entity__) "
        "RETURN s.title AS source, t.title AS target, r"
    )
    print(rel_query)
    try:
        with driver.session(database=db) if db else driver.session() as session:
            node_rows = session.run(node_query).data()
            rel_rows = session.run(rel_query).data()
    finally:
        try:
            driver.close()
        except Exception:
            pass

    # Convert Neo4j node objects to flat dictionaries
    if node_rows:
        flat_node_rows = []
        for row in node_rows:
            node = row["n"]
            flat_row = {"title": node.get("title")}
            # Add all other properties
            for key, value in node.items():
                if key != "title":
                    flat_row[key] = value
            flat_node_rows.append(flat_row)
        entities_df = pd.DataFrame.from_records(flat_node_rows)
    else:
        entities_df = pd.DataFrame.from_records([], columns=["title"])  # type: ignore[arg-type]

    # Convert Neo4j relationship objects to flat dictionaries
    if rel_rows:
        flat_rel_rows = []
        for idx, row in enumerate(rel_rows):
            rel = row["r"]
            flat_row = {
                "source": row["source"],
                "target": row["target"],
                "id": f"neo4j_rel_{idx}",  # Generate unique ID
                "human_readable_id": idx,  # Use index as human readable ID
                "weight": 1.0,  # Default weight
                "combined_degree": 1,  # Default combined degree for ranking
                "description": "",  # Empty description
                "text_unit_ids": []  # Empty text unit IDs
            }
            # Add all relationship properties from Neo4j
            # Neo4j relationship objects have a 'type' property and properties dict
            if hasattr(rel, 'type'):
                flat_row["type"] = rel.type
            if hasattr(rel, 'properties'):
                for key, value in rel.properties.items():
                    flat_row[key] = value
            elif hasattr(rel, 'items'):
                # Fallback for dict-like objects
                for key, value in rel.items():
                    flat_row[key] = value
            flat_rel_rows.append(flat_row)
        relationships_df = pd.DataFrame.from_records(flat_rel_rows)
    else:
        relationships_df = pd.DataFrame.from_records([], columns=["source", "target", "id", "human_readable_id", "weight", "combined_degree", "description", "text_unit_ids"])  # type: ignore[arg-type]
    return entities_df, relationships_df


def _resolve_output_files(
    config: GraphRagConfig,
    output_list: list[str],
    optional_list: list[str] | None = None,
) -> dict[str, Any]:
    """Read indexing output files to a dataframe dict."""
    dataframe_dict = {}

    # Check if Neo4j backend is enabled
    neo4j_enabled = os.getenv("GRAPHRAG_QUERY_BACKEND", "").lower() == "neo4j"
    neo4j_result = None
    
    if neo4j_enabled:
        neo4j_result = _load_entities_relationships_from_neo4j()
        if neo4j_result is None:
            logger.warning("Neo4j backend enabled but failed to load data. Falling back to Parquet.")
            neo4j_enabled = False

    # Loading output files for multi-index search
    if config.outputs:
        dataframe_dict["multi-index"] = True
        dataframe_dict["num_indexes"] = len(config.outputs)
        dataframe_dict["index_names"] = config.outputs.keys()
        
        if neo4j_enabled and neo4j_result is not None:
            # Neo4j mode: use Neo4j data directly for entities/relationships, load other files from Parquet
            for name in output_list:
                if name in ("entities", "relationships"):
                    # Use Neo4j data directly
                    neo4j_df = neo4j_result[0] if name == "entities" else neo4j_result[1]
                    dataframe_dict[name] = [neo4j_df] * len(config.outputs)  # Replicate for each index
                else:
                    # Load other files from Parquet for each index
                    dataframe_dict[name] = []
                    for output in config.outputs.values():
                        storage_obj = create_storage_from_config(output)
                        df_value = asyncio.run(
                            load_table_from_storage(name=name, storage=storage_obj)
                        )
                        dataframe_dict[name].append(df_value)
            
            # Load optional files from Parquet
            if optional_list:
                for optional_file in optional_list:
                    dataframe_dict[optional_file] = []
                    for output in config.outputs.values():
                        storage_obj = create_storage_from_config(output)
                        file_exists = asyncio.run(
                            storage_has_table(optional_file, storage_obj)
                        )
                        if file_exists:
                            df_value = asyncio.run(
                                load_table_from_storage(
                                    name=optional_file, storage=storage_obj
                                )
                            )
                            dataframe_dict[optional_file].append(df_value)
                        else:
                            dataframe_dict[optional_file].append(None)
        else:
            # Parquet mode: load all files from Parquet
            for output in config.outputs.values():
                storage_obj = create_storage_from_config(output)
                for name in output_list:
                    if name not in dataframe_dict:
                        dataframe_dict[name] = []
                    df_value = asyncio.run(
                        load_table_from_storage(name=name, storage=storage_obj)
                    )
                    dataframe_dict[name].append(df_value)

                # for optional output files, do not append if the dataframe does not exist
                if optional_list:
                    for optional_file in optional_list:
                        if optional_file not in dataframe_dict:
                            dataframe_dict[optional_file] = []
                        file_exists = asyncio.run(
                            storage_has_table(optional_file, storage_obj)
                        )
                        if file_exists:
                            df_value = asyncio.run(
                                load_table_from_storage(
                                    name=optional_file, storage=storage_obj
                                )
                            )
                            dataframe_dict[optional_file].append(df_value)
        return dataframe_dict
    # Loading output files for single-index search
    dataframe_dict["multi-index"] = False
    
    if neo4j_enabled and neo4j_result is not None:
        # Neo4j mode: use Neo4j data directly for entities/relationships, load other files from Parquet
        for name in output_list:
            if name in ("entities", "relationships"):
                # Use Neo4j data directly
                neo4j_df = neo4j_result[0] if name == "entities" else neo4j_result[1]
                dataframe_dict[name] = neo4j_df
            else:
                # Load other files from Parquet
                storage_obj = create_storage_from_config(config.output)
                df_value = asyncio.run(load_table_from_storage(name=name, storage=storage_obj))
                dataframe_dict[name] = df_value
        
        # Load optional files from Parquet
        if optional_list:
            storage_obj = create_storage_from_config(config.output)
            for optional_file in optional_list:
                file_exists = asyncio.run(storage_has_table(optional_file, storage_obj))
                if file_exists:
                    df_value = asyncio.run(
                        load_table_from_storage(name=optional_file, storage=storage_obj)
                    )
                    dataframe_dict[optional_file] = df_value
                else:
                    dataframe_dict[optional_file] = None
    else:
        # Parquet mode: load all files from Parquet
        storage_obj = create_storage_from_config(config.output)
        for name in output_list:
            df_value = asyncio.run(load_table_from_storage(name=name, storage=storage_obj))
            dataframe_dict[name] = df_value

        # for optional output files, set the dict entry to None instead of erroring out if it does not exist
        if optional_list:
            for optional_file in optional_list:
                file_exists = asyncio.run(storage_has_table(optional_file, storage_obj))
                if file_exists:
                    df_value = asyncio.run(
                        load_table_from_storage(name=optional_file, storage=storage_obj)
                    )
                    dataframe_dict[optional_file] = df_value
                else:
                    dataframe_dict[optional_file] = None
    
    return dataframe_dict
