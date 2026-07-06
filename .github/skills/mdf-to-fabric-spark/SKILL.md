---
name: mdf-to-fabric-spark
description: 'Convert Azure Data Factory / Synapse Mapping Data Flow (MDF) scripts — the Data Flow Script (DFS) code behind a mapping data flow — into equivalent PySpark notebooks that run on the Microsoft Fabric Spark runtime. USE FOR: "convert mapping data flow to spark", "MDF to Fabric notebook", "migrate ADF data flow to PySpark", "translate data flow script to Spark", "dataflow script to notebook", "convert DFS to PySpark", "Synapse data flow to Fabric", "rewrite mapping data flow as Spark". DO NOT USE FOR: converting ADF pipelines/activities (only the data flow transformation graph), Copy activity migration, or non-dataflow ADF artifacts.'
argument-hint: 'Path to the .txt/.json MDF script (or paste the DFS), plus target Fabric lakehouse/table info if known'
---

# Mapping Data Flow (MDF) → Fabric Spark Notebook Converter

Convert the **Data Flow Script (DFS)** behind an Azure Data Factory / Azure Synapse
**mapping data flow** into an equivalent, runnable **Microsoft Fabric PySpark notebook**.

This skill uses semantic (LLM-driven) translation rather than a fixed parser, so it
handles the full breadth of MDF transformations and expression functions — not just the
limited subset supported by older programmatic tools. It produces idiomatic PySpark
(DataFrame API) plus a conversion report that flags anything requiring human review.

## When to use

- The user has DFS code (from the ADF/Synapse **Script** button, an ARM/JSON `Microsoft.DataFactory/factories/dataflows` resource, or the Data Flow GET REST API) and wants it running on Fabric Spark.
- Migrating ADF/Synapse mapping data flows to Fabric notebooks or Spark Job Definitions.
- Understanding or documenting what an existing data flow does, transformation by transformation.

Not for: ADF **pipeline** activities (Copy, ForEach, Lookup activity, etc.), triggers, linked services, or wrangling data flows (Power Query). This skill only covers the mapping **data flow** transformation graph.

## Core mental model

A DFS is a directed acyclic graph (DAG) of named **streams**. Each statement has the shape:

```
<inputStream(s)> transformation( <properties> ) ~> <outputStreamName>
```

- `source(...)` and `sink(...)` are the graph's entry/exit points.
- `~>` names the resulting stream. That name is the input to a downstream transformation.
- A stream name used as input to two transformations = a **new branch** (reuse the same DataFrame).
- Multiple inputs before a transformation (e.g. `a, b join(...)`) = a multi-input transform.

Translation strategy: **one MDF stream → one PySpark DataFrame variable of the same name.** This keeps the notebook readable and makes it trivial to map the graph back to the original.

## Procedure

Follow these steps in order. Load the referenced files only when you reach the step that needs them.

### 1. Acquire and normalize the DFS

- Read the script from the file/paste the user provides. If it is ARM/JSON, extract the `properties.typeProperties.script` (or `scriptLines`) string and unescape it into multi-line DFS.
- If the script was collapsed to a single line (for PowerShell/API), re-expand it: split on the `~>` stream terminators and transformation boundaries so each transformation is readable.
- Confirm you have the **complete** graph (every stream referenced as an input is defined somewhere).

### 2. Parse the graph

- Split the DFS into individual transformation statements. The optional helper [parse_mdf.py](./scripts/parse_mdf.py) tokenizes the DFS into `{inputs, type, body, output}` blocks and prints the DAG in topological order — run it to get a reliable ordering for large flows.
- Read [references/mdf-script-grammar.md](./references/mdf-script-grammar.md) to correctly handle nested parentheses, `output(...)` schema blocks, quoting, parameters, and the `stream@column` disambiguation syntax.
- Build an ordered list of transformations (sources first, following `~>` edges to sinks). Note branch points and multi-input joins/unions.

### 3. Map each transformation to PySpark

- For every transformation, look up its PySpark equivalent in [references/transformation-mapping.md](./references/transformation-mapping.md). It covers source, sink, select/mapColumn, filter, derive, aggregate, join, lookup, exists, union, conditionalSplit, window, rank, surrogateKey/keyGenerate, sort, alterRow, pivot, unpivot, flatten, parse, cast, stringify, flowlet, assert, and more.
- Translate every expression (column formulas, filter/join conditions, aggregations) using [references/expression-function-mapping.md](./references/expression-function-mapping.md), which maps the MDF expression language (`iif`, `coalesce`, `toTimestamp`, `sha2`, `$$`, `match(...)`, `columns()`, `byName`, etc.) to `pyspark.sql.functions` / Spark SQL.
- Preserve column order and data types. MDF `output(...)` blocks and `select(mapColumn(...))` define the exact projected schema — honor it.

### 4. Handle sources, sinks, and parameters for Fabric

- Read [references/fabric-notebook-guide.md](./references/fabric-notebook-guide.md) to translate source/sink datasets into Fabric lakehouse reads/writes (`abfss://` paths, `/lakehouse/default/Files|Tables`, Delta tables), and to structure the notebook (spark session, imports, one cell per logical stage).
- Read [references/parameters-and-config.md](./references/parameters-and-config.md) to turn the MDF `parameters{...}` block and dataset parameters into notebook parameter cells (`%%configure` / a tagged parameters cell / `mssparkutils` widgets) with the original defaults.

### 5. Assemble the notebook

- Build the `.ipynb` from [assets/notebook-template.ipynb](./assets/notebook-template.ipynb): parameters cell → imports/session → source cells → transformation cells (in DAG order) → sink cells → optional validation cell.
- Keep the DataFrame variable names identical to the MDF stream names. Add a short markdown cell before each transformation showing the original DFS line, so the notebook is self-documenting.
- Follow the fully worked reference conversion in [examples/wideworldimporters-city-sales.md](./examples/wideworldimporters-city-sales.md) for the expected structure, naming, and comment style.
- If the user wants a plain script or a Spark Job Definition instead of a notebook, emit a single `.py` file with the same cell contents concatenated and an explicit `SparkSession` bootstrap.

### 6. Validate and report

- Read [references/validation-and-testing.md](./references/validation-and-testing.md) and add the schema/row-count/null-check assertions it describes so the user can confirm parity against the original data flow output.
- Produce a conversion report from [assets/conversion-report-template.md](./assets/conversion-report-template.md): list every transformation, its PySpark mapping, and — critically — any **manual-review items** (see below).

## Things that need explicit human review (always flag these)

MDF and Spark are not 1:1. Call these out in the report rather than silently guessing:

- **Schema drift** (`allowSchemaDrift: true`): Spark has no runtime drift; you must either read all columns or handle late-arriving columns explicitly.
- **`byName`/`byNames`/`columns()`/`match()` pattern rules**: these depend on the runtime schema; verify the generated dynamic column logic against real data.
- **AlterRow + sink upsert/delete/update policies**: map to Delta `MERGE`; the key columns and policy order must be confirmed.
- **Join `broadcast` hints, `partitionBy`/optimize settings, and `sort` semantics**: Spark ordering is not guaranteed after shuffles unless re-sorted before the sink.
- **Data type nuances**: MDF `decimal(p,s)`, `short`, `timestamp` parsing formats, and implicit casts differ from Spark; verify `toTimestamp`/`toDate` format strings.
- **Cached lookups / `sinkCache`, `external call`, `assert`, `flowlet`**: confirm the chosen Spark equivalent (broadcast join, UDF/REST call, `raise`, inlined function).
- **Surrogate keys**: `monotonically_increasing_id()` is not sequential/gap-free like MDF's key generator — use `row_number()` over a window if sequential keys matter.

## Output conventions

- Default output: a Fabric-ready PySpark `.ipynb` next to the source, named after the data flow (e.g. `WideWorldImportersDW_City_Sales.ipynb`), plus `*_conversion_report.md`.
- Never fabricate lakehouse/workspace IDs or dataset paths. If a source/sink path is unknown, emit a clearly-marked `TODO` parameter with a placeholder and note it in the report.
- Prefer the DataFrame API over raw SQL strings for maintainability, but use `spark.sql` when it more faithfully mirrors a complex MDF expression.

## Skill resources

| File | Use for |
|------|---------|
| [references/mdf-script-grammar.md](./references/mdf-script-grammar.md) | Parsing the DFS: streams, `~>`, nested bodies, `output()`, `mapColumn`, split outputs |
| [references/transformation-mapping.md](./references/transformation-mapping.md) | Every MDF transformation → PySpark |
| [references/expression-function-mapping.md](./references/expression-function-mapping.md) | MDF expression language → `pyspark.sql.functions` / Spark SQL |
| [references/fabric-notebook-guide.md](./references/fabric-notebook-guide.md) | Fabric session, lakehouse paths, read/write, notebook layout |
| [references/parameters-and-config.md](./references/parameters-and-config.md) | `parameters{}` block, dataset params, secrets |
| [references/validation-and-testing.md](./references/validation-and-testing.md) | Parity checks and common failure fixes |
| [scripts/parse_mdf.py](./scripts/parse_mdf.py) | Tokenize a DFS into ordered transformation blocks |
| [assets/notebook-template.ipynb](./assets/notebook-template.ipynb) | Fabric PySpark notebook skeleton |
| [assets/conversion-report-template.md](./assets/conversion-report-template.md) | Conversion report structure |
| [examples/wideworldimporters-city-sales.md](./examples/wideworldimporters-city-sales.md) | Full worked conversion (reference quality) |
