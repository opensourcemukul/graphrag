# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""CLI implementation of the query subcommand."""

import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import graphrag.api as api
from graphrag.callbacks.noop_query_callbacks import NoopQueryCallbacks
from graphrag.config.load_config import load_config
from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.utils.api import create_storage_from_config
from graphrag.utils.storage import load_table_from_storage, storage_has_table
import os
import pandas as pd

if TYPE_CHECKING:
    import pandas as pd

# ruff: noqa: T201


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

    node_query = "MATCH (n:__Entity__) RETURN n.title AS title"
    rel_query = (
        "MATCH (s:__Entity__)-[r:RELATED]->(t:__Entity__) "
        "RETURN s.title AS source, t.title AS target, r.weight AS weight"
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

    entities_df = (
        pd.DataFrame.from_records(node_rows)
        if node_rows
        else pd.DataFrame.from_records([], columns=["title"])  # type: ignore[arg-type]
    )
    relationships_df = (
        pd.DataFrame.from_records(rel_rows)
        if rel_rows
        else pd.DataFrame.from_records([], columns=["source", "target", "weight"])  # type: ignore[arg-type]
    )
    return entities_df, relationships_df


def _resolve_output_files(
    config: GraphRagConfig,
    output_list: list[str],
    optional_list: list[str] | None = None,
) -> dict[str, Any]:
    """Read indexing output files to a dataframe dict."""
    dataframe_dict = {}

    # Attempt Neo4j override for entities/relationships if enabled
    neo4j_result = _load_entities_relationships_from_neo4j()

    # Loading output files for multi-index search
    if config.outputs:
        dataframe_dict["multi-index"] = True
        dataframe_dict["num_indexes"] = len(config.outputs)
        dataframe_dict["index_names"] = config.outputs.keys()
        for output in config.outputs.values():
            storage_obj = create_storage_from_config(output)
            for name in output_list:
                if name not in dataframe_dict:
                    dataframe_dict[name] = []
                df_value = asyncio.run(
                    load_table_from_storage(name=name, storage=storage_obj)
                )
                # If Neo4j is enabled, override only after load for entities/relationships
                if neo4j_result and name in ("entities", "relationships"):
                    if len(dataframe_dict[name]) == 0:
                        override_df = neo4j_result[0] if name == "entities" else neo4j_result[1]
                        if name == "entities":
                            # Use Parquet entities schema and filter by Neo4j titles to preserve required columns
                            try:
                                if not override_df.empty and "title" in df_value.columns:
                                    titles = set(override_df["title"].dropna().astype(str))
                                    filtered = df_value[df_value["title"].astype(str).isin(list(titles))]
                                    # Fallback if filter results empty
                                    override_df = filtered if not filtered.empty else df_value
                                else:
                                    override_df = df_value
                            except Exception:
                                override_df = df_value
                        else:
                            # relationships: filter Parquet by Neo4j source/target pairs to preserve schema
                            try:
                                if not override_df.empty and {"source", "target"}.issubset(df_value.columns):
                                    # Normalize strings for matching
                                    neo_src = override_df["source"].dropna().astype(str)
                                    neo_tgt = override_df["target"].dropna().astype(str)
                                    neo_pairs = set(zip(neo_src, neo_tgt))
                                    filtered = df_value[
                                        df_value[["source", "target"]]
                                        .astype(str)
                                        .apply(tuple, axis=1)
                                        .isin(list(neo_pairs))
                                    ]
                                    override_df = filtered if not filtered.empty else df_value
                                else:
                                    override_df = df_value
                            except Exception:
                                override_df = df_value
                        dataframe_dict[name].append(override_df)
                    else:
                        dataframe_dict[name].append(df_value)
                else:
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
    storage_obj = create_storage_from_config(config.output)
    for name in output_list:
        df_value = asyncio.run(load_table_from_storage(name=name, storage=storage_obj))
        if neo4j_result and name == "entities":
            override_df = neo4j_result[0]
            try:
                if not override_df.empty and "title" in df_value.columns:
                    titles = set(override_df["title"].dropna().astype(str))
                    filtered = df_value[df_value["title"].astype(str).isin(list(titles))]
                    override_df = filtered if not filtered.empty else df_value
                else:
                    override_df = df_value
            except Exception:
                override_df = df_value
            dataframe_dict[name] = override_df
        elif neo4j_result and name == "relationships":
            override_df = neo4j_result[1]
            try:
                if not override_df.empty and {"source", "target"}.issubset(df_value.columns):
                    neo_src = override_df["source"].dropna().astype(str)
                    neo_tgt = override_df["target"].dropna().astype(str)
                    neo_pairs = set(zip(neo_src, neo_tgt))
                    filtered = df_value[
                        df_value[["source", "target"]]
                        .astype(str)
                        .apply(tuple, axis=1)
                        .isin(list(neo_pairs))
                    ]
                    override_df = filtered if not filtered.empty else df_value
                else:
                    override_df = df_value
            except Exception:
                override_df = df_value
            dataframe_dict[name] = override_df
        else:
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
