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

    def write_nodes(tx, rows: list[dict[str, Any]]):  # type: ignore[no-untyped-def]
        for row in rows:
            # Build dynamic MERGE query with all properties
            title = row["title"]
            properties = {k: v for k, v in row.items() if k != "title" and v is not None}
            
            # Create SET clause for all properties
            set_clauses = []
            for prop, value in properties.items():
                # Escape property names and handle different data types
                escaped_prop = f"`{prop}`" if " " in prop or "-" in prop else prop
                set_clauses.append(f"n.{escaped_prop} = ${prop}")
            
            set_clause = ", ".join(set_clauses) if set_clauses else "n.createdAt = timestamp()"
            
            query = f"MERGE (n:__Entity__ {{title: $title}}) SET {set_clause} RETURN n"
            
            params = {"title": title, **properties}
            tx.run(query, **params)  # type: ignore[no-untyped-call]

    def write_rels(tx, rows: list[dict[str, Any]]):  # type: ignore[no-untyped-def]
        for row in rows:
            source = row["source"]
            target = row["target"]
            properties = {k: v for k, v in row.items() if k not in ["source", "target"] and v is not None}
            
            # Create SET clause for all properties
            set_clauses = []
            for prop, value in properties.items():
                escaped_prop = f"`{prop}`" if " " in prop or "-" in prop else prop
                set_clauses.append(f"r.{escaped_prop} = ${prop}")
            
            set_clause = ", ".join(set_clauses) if set_clauses else "r.weight = coalesce($weight, r.weight)"
            
            query = (
                "MERGE (s:__Entity__ {title: $source}) "
                "MERGE (t:__Entity__ {title: $target}) "
                f"MERGE (s)-[r:RELATED]->(t) SET {set_clause} RETURN r"
            )
            
            params = {"source": source, "target": target, **properties}
            tx.run(query, **params)  # type: ignore[no-untyped-call]

    def _batch_iter(df: pd.DataFrame, cols: list[str], rename: dict[str, str] | None = None):
        r = df[cols].rename(columns=rename or {}).to_dict(orient="records")
        for i in range(0, len(r), batch_size):
            yield r[i : i + batch_size]

    try:
        with driver.session(database=database) if database else driver.session() as session:
            # Nodes - include ALL columns from entities DataFrame
            entity_cols = list(entities.columns)
            for batch in _batch_iter(entities, entity_cols):
                session.execute_write(write_nodes, batch)  # type: ignore[arg-type]

            # Relationships - include ALL columns from relationships DataFrame
            rel_df = relationships.copy()
            # Coerce to strings for endpoints; other columns keep their types
            rel_df["source"] = rel_df["source"].map(_coerce_str)
            rel_df["target"] = rel_df["target"].map(_coerce_str)
            
            rel_cols = list(rel_df.columns)
            for batch in _batch_iter(rel_df, rel_cols):
                session.execute_write(write_rels, batch)  # type: ignore[arg-type]
    finally:
        try:
            driver.close()
        except Exception:
            pass
