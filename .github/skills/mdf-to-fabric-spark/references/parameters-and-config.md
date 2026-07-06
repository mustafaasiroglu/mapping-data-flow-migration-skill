# Parameters & Configuration Mapping

How to translate the MDF `parameters{...}` block, dataset parameters, and data flow locals
into a Fabric notebook parameters cell.

---

## 1. The MDF `parameters{...}` block

```
parameters{
    Container as string ("data"),
    CityFolderPath as string ("WideWorldImportersDW/parquet/full/dimension_city"),
    StateFilter1 as string ("Texas"),
    StateFilter2 as string ("California")
}
```

Each entry is `Name as type (default)`. Referenced elsewhere as `$Name`. Map to plain Python
variables in a **single parameters cell**, keeping the original defaults:

```python
# --- Parameters (tag this cell "parameters" for Fabric/Papermill) ---
Container        = "data"
CityFolderPath   = "WideWorldImportersDW/parquet/full/dimension_city"
SalesFolderPath  = "WideWorldImportersDW/parquet/full/fact_sale"
SinkContainer    = "data"
SinkFolderPath   = "SparkTestResult"
StateFilter1     = "Texas"
StateFilter2     = "California"

# --- Storage / Fabric config (not in the MDF; set for your environment) ---
STORAGE_ACCOUNT  = "TODO_set_me"      # ADLS Gen2 account, or use OneLake below
USE_ONELAKE      = False              # True to read/write via the attached lakehouse
```

Then every `$Name` in the DFS becomes the Python variable `Name`.

### Type mapping for parameter values

| MDF type | Python literal |
|---|---|
| `string` | `"..."` |
| `integer` / `long` / `short` | `int` |
| `double` / `float` | `float` |
| `boolean` | `True` / `False` |
| `timestamp` / `date` | ISO string, parsed where used with `F.to_timestamp/to_date` |
| `array` | Python `list` |
| `object` / `map` | Python `dict` |

---

## 2. Making parameters overridable in Fabric

Tag the parameters cell so it can be overridden at run time (Fabric pipeline "Notebook"
activity, `mssparkutils.notebook.run`, or the REST/job API):

- In the `.ipynb`, add `"tags": ["parameters"]` to the cell metadata.
- Fabric/Papermill injects an override cell **right after** the tagged cell, so downstream
  code always sees the overridden values.

Optional widget-style reads (when driven by a pipeline that passes parameters):

```python
# Values passed from a Fabric pipeline Notebook activity arrive as the tagged-cell overrides.
# For ad-hoc reads you can also use exit/return values via mssparkutils.notebook.exit(...).
```

---

## 3. Dataset parameters

ADF datasets often have their own parameters (e.g. a parameterized `folderPath` or
`fileName`). These arrive in the DFS as source/sink properties that reference `$flowParam`
or literal expressions. Resolve them into the path-building code:

```python
folder = f"{CityFolderPath}"                       # from flow param
path   = f"{base_path}/{folder}"
dimcity = spark.read.parquet(path)
```

If a dataset builds a path from an expression (e.g. `@concat('year=', pipeline().year)`),
reproduce the expression in Python and expose its inputs as parameters.

---

## 4. Data flow locals & cached lookups

- **Locals** (referenced as `:localName`) are reusable expressions defined once. Compute them
  as Python variables / small DataFrames before the stages that use them.
- **Cached sinks used as lookups**: materialize once, `.cache()`, and reuse:

```python
lookup_df = some_stage.cache()
lookup_df.count()   # force materialization once
```

---

## 5. Global settings

Put any run-wide Spark config near the top (after imports), only when needed:

```python
# Example: only if legacy date parsing is required to match MDF format strings
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
```

Avoid setting configs the Fabric runtime already handles well (AQE, shuffle partitions)
unless you are matching a specific MDF optimize/partition setting.

---

## 6. Secrets & credentials

- Never hardcode account keys or SAS tokens. For external ADLS, prefer workspace identity /
  OneLake shortcuts, or read secrets via `mssparkutils.credentials.getSecret(akv, name)`.
- Note any credential requirement in the conversion report so the user wires it up securely.
