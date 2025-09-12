# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing snapshot_neo4j method definition."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


async def snapshot_neo4j(
    entities: pd.DataFrame,
    relationships: pd.DataFrame,
    *,
    uri: str,
    username: str,
    password: str,
    database: str | None = None,
    batch_size: int = 1000,
) -> None:
    """Write entities and relationships to Neo4j.

    This function performs a minimal idempotent MERGE of nodes and relationships.
    It expects `entities` to contain a `title` column used as the node key,
    and `relationships` to contain `source`, `target`, and optionally `weight` columns.

    If the `neo4j` driver is not installed, it logs a warning and returns.
    """
    try:
        from neo4j import GraphDatabase  # type: ignore
    except Exception:  # pragma: no cover - optional dependency
        logger.warning(
            "neo4j driver not installed; skipping Neo4j snapshot. Install 'neo4j' to enable."
        )
        return

    def _coerce_str(value: Any) -> str | None:
        if value is None:
            return None
        try:
            return str(value)
        except Exception:
            return None

    if "title" not in entities.columns:
        logger.warning(
            "snapshot_neo4j: entities DataFrame missing 'title' column; skipping node import."
        )
        return

    if not {"source", "target"}.issubset(set(relationships.columns)):
        logger.warning(
            "snapshot_neo4j: relationships DataFrame missing 'source'/'target'; skipping relationship import."
        )
        return

    driver = GraphDatabase.driver(uri, auth=(username, password))

    node_query = (
        "MERGE (n:__Entity__ {title: $title}) "
        "ON CREATE SET n.createdAt = timestamp() "
        "RETURN n"
    )

    rel_query = (
        "MERGE (s:__Entity__ {title: $source}) "
        "MERGE (t:__Entity__ {title: $target}) "
        "MERGE (s)-[r:RELATED]->(t) "
        "SET r.weight = coalesce($weight, r.weight) "
        "RETURN r"
    )

    def write_nodes(tx, rows: list[dict[str, Any]]):  # type: ignore[no-untyped-def]
        for row in rows:
            tx.run(node_query, title=row["title"])  # type: ignore[no-untyped-call]

    def write_rels(tx, rows: list[dict[str, Any]]):  # type: ignore[no-untyped-def]
        for row in rows:
            tx.run(  # type: ignore[no-untyped-call]
                rel_query,
                source=row["source"],
                target=row["target"],
                weight=row.get("weight"),
            )

    def _batch_iter(df: pd.DataFrame, cols: list[str], rename: dict[str, str] | None = None):
        r = df[cols].rename(columns=rename or {}).to_dict(orient="records")
        for i in range(0, len(r), batch_size):
            yield r[i : i + batch_size]

    try:
        with driver.session(database=database) if database else driver.session() as session:
            # Nodes
            for batch in _batch_iter(entities, ["title"]):
                session.execute_write(write_nodes, batch)  # type: ignore[arg-type]

            # Relationships
            rel_cols = ["source", "target"] + (["weight"] if "weight" in relationships.columns else [])
            # Coerce to strings for endpoints; weight may be numeric
            rel_df = relationships.copy()
            rel_df["source"] = rel_df["source"].map(_coerce_str)
            rel_df["target"] = rel_df["target"].map(_coerce_str)
            if "weight" in rel_df.columns:
                # keep as-is; Neo4j accepts numeric
                pass

            for batch in _batch_iter(rel_df, rel_cols):
                session.execute_write(write_rels, batch)  # type: ignore[arg-type]
    finally:
        try:
            driver.close()
        except Exception:
            pass
