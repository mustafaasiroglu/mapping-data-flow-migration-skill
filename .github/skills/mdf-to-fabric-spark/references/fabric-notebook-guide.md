# Microsoft Fabric Notebook Guide

How to shape the converted code so it runs cleanly on the **Fabric Spark runtime**, and how
to translate ADF/Synapse datasets into Fabric lakehouse reads/writes.

---

## 1. The Fabric Spark session

In a Fabric notebook the `spark` session already exists — **do not** create a new
`SparkSession` in a notebook. Only create one in a standalone `.py` / Spark Job Definition:

```python
# Only for a standalone script / Spark Job Definition (NOT inside a Fabric notebook):
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("<DataFlowName>").getOrCreate()
```

`mssparkutils` (a.k.a. `notebookutils`) is available for filesystem, credentials, and
parameters:

```python
import mssparkutils            # legacy name, still supported
# or:  import notebookutils     # newer alias
```

---

## 2. Storage paths — from ADF datasets to Fabric

ADF/Synapse sources/sinks reference containers + folder paths in ADLS Gen2. In Fabric you
target a **Lakehouse**. Three ways to reference data:

### a) Lakehouse-relative paths (simplest when the notebook is attached to a lakehouse)

```python
# Files area (unmanaged, e.g. parquet/csv dropped in the lake):
files_base = "/lakehouse/default/Files"           # local FS mount
df = spark.read.parquet(f"{files_base}/{folder_path}")

# Managed tables (Delta):
df = spark.read.table("MyLakehouse.dim_city")      # or spark.read.table("dim_city")
```

### b) ABFSS paths (explicit, works cross-workspace / external ADLS)

```python
# Fabric OneLake:
abfss = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files"
df = spark.read.parquet(f"{abfss}/{folder_path}")

# External ADLS Gen2 (same as ADF source):
adls = "abfss://<container>@<account>.dfs.core.windows.net"
df = spark.read.parquet(f"{adls}/{folder_path}")
```

### c) Mapping the MDF source/sink properties

Given `fileSystem: ($Container)` and `folderPath: ($CityFolderPath)`, build:

```python
Container = "data"                       # from parameters / widget
CityFolderPath = "WideWorldImportersDW/parquet/full/dimension_city"
base_path = "abfss://" + Container + "@<account>.dfs.core.windows.net"   # external ADLS
# OR target OneLake if the data has been landed in the lakehouse.
dimcity = spark.read.format("parquet").load(f"{base_path}/{CityFolderPath}")
```

> If you don't know the storage account / lakehouse, emit a clearly-labeled `TODO`
> parameter (e.g. `STORAGE_ACCOUNT = "TODO_set_me"`) and note it in the conversion report.
> **Never invent** account names, workspace IDs, or lakehouse IDs.

---

## 3. Read/write format cheat-sheet

```python
# Parquet
spark.read.parquet(path)
df.write.mode("overwrite").parquet(path)

# Delta (preferred sink for lakehouse tables)
spark.read.format("delta").load(path)          # by path
spark.read.table("lakehouse.table")            # by name
df.write.format("delta").mode("overwrite").saveAsTable("lakehouse.table")

# CSV (carry over ADF dataset options)
spark.read.option("header", True).option("sep", ",").csv(path)
df.write.option("header", True).mode("overwrite").csv(path)

# JSON
spark.read.option("multiLine", True).json(path)
```

Write modes: map the sink's behavior — recreate/overwrite→`overwrite`, append→`append`,
error→`errorifexists`, ignore→`ignore`. For truncate-and-load use `overwrite`. For
insert/update/delete use Delta `MERGE` (see AlterRow in the transformation reference).

---

## 4. Notebook structure (cell layout)

Produce cells in this order so the notebook reads top-to-bottom like the original DAG:

1. **Markdown title cell** — data flow name, source, generated-by note, link back to DFS.
2. **Parameters cell** — tagged `parameters` (see parameters-and-config.md). Holds every MDF `parameters{}` default plus storage/lakehouse config.
3. **Imports cell** — `from pyspark.sql import functions as F, Window` / `from pyspark.sql.types import *` / `from delta.tables import DeltaTable` (only if merging).
4. **Source cells** — one per `source`, each preceded by a short markdown cell echoing the DFS line.
5. **Transformation cells** — one per transformation, in topological order, each preceded by a markdown cell with the original DFS snippet. Keep DataFrame variable = MDF stream name.
6. **Sink cells** — one per `sink`.
7. **Validation cell** (optional) — schema/row-count checks (see validation-and-testing.md).

Keep each transformation in its own cell so users can run/inspect stage by stage — this
mirrors the "debug row preview" experience of the MDF designer.

---

## 5. Fabric-specific performance & correctness notes

- **AQE is on by default** in Fabric; you rarely need manual `repartition`. Preserve any MDF `partitionBy` on the sink for downstream layout.
- **Broadcast joins**: honor MDF `broadcast` hints with `F.broadcast(dim)`. For small dims Fabric will often auto-broadcast anyway.
- **Ordering**: `sort` results are lost after subsequent shuffles. If the sink must be ordered, `orderBy` immediately before the write (and accept that file-level order across partitions isn't globally guaranteed unless you `coalesce(1)`).
- **V-Order / optimize**: Fabric writes V-Ordered Delta by default; no action needed to match MDF "optimize" sink settings in most cases.
- **Timezone / date parsing**: set `spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")` only if MDF format strings fail under the modern parser. Prefer fixing the pattern.
- **Decimal precision**: Spark arithmetic can change decimal scale; cast explicitly to match the MDF `output()` types before the sink.
- **Schema drift**: Spark has no runtime drift. If `allowSchemaDrift: true`, read all source columns (don't pin a projection) and use `unionByName(allowMissingColumns=True)`; flag it.

---

## 6. Emitting the notebook file

Fabric imports standard Jupyter `.ipynb`. Build the notebook JSON from
[../assets/notebook-template.ipynb](../assets/notebook-template.ipynb):

- `metadata.language_info.name = "python"`.
- Tag the parameters cell with `"tags": ["parameters"]` so Fabric/Papermill treats it as the parameter cell.
- Optionally add Fabric lakehouse binding under `metadata.dependencies.lakehouse` (workspace/lakehouse IDs) — leave placeholders if unknown and note in the report.
- For a **Spark Job Definition** instead, emit a single `.py` with the same code plus the explicit `SparkSession` bootstrap from section 1.

## 7. Attaching a lakehouse

Remind the user to attach the target lakehouse to the notebook (or set the default lakehouse)
so relative `/lakehouse/default/...` paths and `spark.read.table(...)` resolve. If they use
only `abfss://` paths, attachment is optional.
