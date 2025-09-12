### What we changed and why (Neo4j + Query Backend Switch)

This document explains the improvements we added to support using Neo4j alongside the default Parquet-based GraphRAG workflow, with a focus on didactic value: what changed, why it matters, and how to use it safely without breaking existing functionality.

---

## 1) Non-breaking graph export to Neo4j during indexing

- We introduced a minimal, optional export of the final graph (entities and relationships) into Neo4j at the end of the indexing pipeline.
- This runs during the `finalize_graph` workflow and is fully controlled by environment variables, so default behavior remains unchanged.

Where:
- `graphrag/index/operations/snapshot_neo4j.py` (new)
  - Asynchronously writes nodes and edges to Neo4j using MERGE (idempotent).
  - If the `neo4j` Python driver is not installed, it logs a warning and no-ops.
- `graphrag/index/workflows/finalize_graph.py` (edited)
  - After persisting Parquet artifacts, it conditionally calls `snapshot_neo4j(...)` based on environment variables.

How to enable export:
- Set the following env vars (e.g., in your `.env` next to `settings.yaml`):
  - `GRAPHRAG_NEO4J_ENABLE=true`
  - `GRAPHRAG_NEO4J_URI=neo4j://localhost` (or `neo4j+s://...`)
  - `GRAPHRAG_NEO4J_USERNAME=neo4j`
  - `GRAPHRAG_NEO4J_PASSWORD=your_password`
  - optional: `GRAPHRAG_NEO4J_DATABASE=neo4j`
- Install the driver if needed: `pip install neo4j`

Why this design?
- Keep the existing storage abstraction and Parquet outputs untouched.
- Make Neo4j an optional “snapshot/export” step, preserving backward compatibility.

Didactic takeaway:
- Decoupling persistence (Parquet) from external exports (Neo4j) avoids invasive changes and reduces risk. Feature flags (env vars) allow progressive rollout without breaking defaults.

---

## 2) Toggleable query backend (Parquet by default, Neo4j on demand)

- The built-in `graphrag query` CLI historically reads Parquet artifacts (plus your configured vector store). We added a new option to source graph structure from Neo4j during query time without changing the CLI interface.
- This is also controlled by environment variables and defaults to Parquet mode, ensuring no breakage.

Where:
- `graphrag/cli/query.py` (edited)
  - Adds an internal loader that, when enabled, fetches titles and edges from Neo4j, then aligns them with the Parquet schema used by GraphRAG’s loaders/adapters.

How the override works:
- Entities: we fetch entity titles from Neo4j, then filter the Parquet `entities` table by those titles. This preserves all required columns (e.g., `id`, `human_readable_id`, `description`, embeddings, etc.).
- Relationships: we fetch `(source, target)` pairs from Neo4j, then filter the Parquet `relationships` table by those pairs. This preserves required columns (e.g., `id`, `human_readable_id`, `combined_degree`, etc.).
- If the Neo4j driver or env vars are missing, it silently falls back to Parquet-only behavior.

How to enable Neo4j-backed query:
- In your `.env` next to `settings.yaml`:
  - `GRAPHRAG_QUERY_BACKEND=neo4j`
  - `GRAPHRAG_NEO4J_URI=neo4j://localhost` (or `neo4j+s://...`)
  - `GRAPHRAG_NEO4J_USERNAME=neo4j`
  - `GRAPHRAG_NEO4J_PASSWORD=your_password`
  - optional: `GRAPHRAG_NEO4J_DATABASE=neo4j`
- Then run queries as usual, e.g.:
  - `graphrag query --method GLOBAL --query "your question" --root /path/to/project --data /path/to/project/output/artifacts -v`

Why filter instead of replace?
- GraphRAG’s query adapters expect specific columns in `entities` and `relationships` (e.g., `id`, `human_readable_id`, and other metadata). Raw Neo4j results don’t include these columns. By filtering the Parquet tables using Neo4j-derived keys (titles and edge endpoints), we keep the full schema intact.

Didactic takeaway:
- When integrating a new source with an established data loader, preserve the “shape” (schema) required by downstream code. Use your new source as a selector (filter) rather than a replacer unless you fully re-implement the loader contract.

---

## 3) Robustness and error-proofing

Observed issues and fixes:
- Missing columns (e.g., `id`, `human_readable_id`) in Neo4j-only data caused adapter failures.
  - Fix: keep Parquet as the canonical schema; use Neo4j to select rows via keys (titles for nodes, `(source,target)` for edges).
- Type/lint issues while constructing empty DataFrames.
  - Fix: use `pd.DataFrame.from_records(..., columns=[...])` and simple conversions like `list(...)` before `isin(...)` to satisfy type checkers.

Didactic takeaway:
- Favor graceful degradation: if Neo4j is unavailable or misconfigured, fall back automatically to Parquet to preserve uptime.
- Aim for schema-compatibility at boundaries to minimize churn in downstream logic.

---

## 4) How to run end-to-end

Indexing (default Parquet, optional Neo4j export):
1. Ensure your `.env` is next to `settings.yaml`; it is auto-loaded.
2. Export to Neo4j if desired by setting `GRAPHRAG_NEO4J_ENABLE=true` and connection vars.
3. Run: `graphrag index --root /path/to/project -v`
   - Parquet artifacts land in your configured `output/artifacts`.
   - If enabled, the finalize step pushes nodes/edges to Neo4j.

Querying (switchable backend):
1. Default: no env change → queries use Parquet artifacts.
2. Neo4j mode: set `GRAPHRAG_QUERY_BACKEND=neo4j` and Neo4j connection vars.
3. Run: `graphrag query --method GLOBAL --query "..." --root /path/to/project --data /path/to/project/output/artifacts -v`
   - Entities/relationships are selected via Neo4j content but keep Parquet schema.

---

## 5) Design principles highlighted

- Backward compatibility first: new features default off and don’t alter existing code paths.
- Feature flags via environment variables: quick enable/disable and safe experimentation.
- Schema preservation over data replacement: integrate external stores without rewriting downstream expectations.
- Idempotent writes to graph databases: use MERGE to safely re-run exports.
- Graceful fallback: if any optional dependency is missing, log and proceed with the stable path.

---

## 6) Future extensions (optional)

- Richer Neo4j mapping: persist and read back additional properties (descriptions, types, ranks) when needed.
- Config-driven backend selection: move the query backend flag from env to config if desired.
- Multi-index Neo4j support: today we override the first index; this can be expanded per use case.

---

By following these principles and patterns, we introduced Neo4j capabilities with minimal surface area of change, preserved stability, and provided a clear path to iterate further.


