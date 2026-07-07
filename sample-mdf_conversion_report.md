# Conversion Report — WideWorldImporters City/Sales

**Source DFS:** `sample-mdf.txt`
**Generated notebook:** `WideWorldImportersDW_City_Sales.ipynb`
**Target runtime:** Microsoft Fabric Spark (PySpark)
**Date:** 2026-07-06

---

## 1. Summary

| Metric | Value |
|---|---|
| Total transformations | 9 |
| Sources | 2 (`dimcity`, `sales`) |
| Sinks | 1 (`sinktostorage`) |
| Joins / lookups / unions | 1 (`joinwithcity`, left join) |
| Manual-review items | 5 |
| Overall confidence | High |

The flow reads two parquet datasets from ADLS Gen2 — the `dimension_city` dimension and the
`fact_sale` fact. It filters sales to high-value rows (`TotalExcludingTax > 700` **and**
`Profit > 500`), filters cities to two states (`StateFilter1` / `StateFilter2`, defaulting to
Texas and California), renames/projects columns on each side, left-joins sales to city on
`CityKey`, projects the final 16-column result, and writes it as parquet to
`SinkContainer/SinkFolderPath`.

---

## 2. Parameters mapped

| MDF parameter | Type | Default | Notebook variable |
|---|---|---|---|
| `Container` | string | `data` | `Container` |
| `CityFolderPath` | string | `WideWorldImportersDW/parquet/full/dimension_city` | `CityFolderPath` |
| `SalesFolderPath` | string | `WideWorldImportersDW/parquet/full/fact_sale` | `SalesFolderPath` |
| `SinkContainer` | string | `data` | `SinkContainer` |
| `SinkFolderPath` | string | `SparkTestResult` | `SinkFolderPath` |
| `StateFilter1` | string | `Texas` | `StateFilter1` |
| `StateFilter2` | string | `California` | `StateFilter2` |

**Config the user must set** (not in the MDF):

- `STORAGE_ACCOUNT` — ADLS Gen2 account name. Emitted as `"TODO_set_me"`; **the notebook
  will not run until this is set** (or `USE_ONELAKE = True`). Never invented — see item 5 below.
- `USE_ONELAKE` — set `True` to read/write through the attached Fabric lakehouse `Files` area
  instead of external ADLS.

> **Fabric parameter cell:** the parameters cell holds all seven MDF defaults. VS Code's
> notebook serializer does not persist the Papermill `"tags": ["parameters"]` marker, so after
> importing into Fabric, select the parameters cell and use **… → Toggle parameter cell** so
> pipeline / job overrides are injected there.

---

## 3. Transformation-by-transformation mapping

| # | MDF stream | Type | PySpark mapping | Notes |
|---|---|---|---|---|
| 1 | `dimcity` | source | `spark.read.format("parquet").load(...)` | Reads all columns (schema drift on) |
| 2 | `sales` | source | `spark.read.format("parquet").load(...)` | Reads all columns (schema drift on) |
| 3 | `salesfilter` | filter | `.filter((col("TotalExcludingTax") > 700) & (col("Profit") > 500))` | `&&` → `&`, each comparison parenthesized |
| 4 | `cityfilter` | filter | `.filter((col("StateProvince") == StateFilter1) \| (col("StateProvince") == StateFilter2))` | `\|\|` → `\|`; params inlined as Python vars |
| 5 | `cityselect` | select / mapColumn | `.select(... col("StateProvince").alias("State") ...)` | Rename `State = StateProvince`; 8 cols |
| 6 | `salesselect` | select / mapColumn | `.select(... .alias("InvoiceDate"/"DeliveryDate"/"Salesperson"/"TotalWithoutTax"/"TotalWithTax") ...)` | 5 renames; 12 cols |
| 7 | `joinwithcity` | join | `salesselect.alias("s").join(cityselect.alias("c"), col("s.CityKey")==col("c.CityKey"), how="left")` | `joinType:'left'`; `broadcast:'auto'` left to AQE |
| 8 | `joinselect` | select / mapColumn | `.select(...)` 16 columns | Drops the duplicate `CityKey`; sets final order |
| 9 | `sinktostorage` | sink | `.write.format("parquet").mode("overwrite").save(...)` | No write-behavior in DFS → `overwrite` |

Stream → DataFrame variable names are preserved 1:1, and cells are ordered in the DAG
topological order produced by `parse_mdf.py`.

---

## 4. Manual-review items (⚠️ verify these)

1. **[Storage path / credentials]** `fileSystem: ($Container)` + `folderPath` were external
   ADLS Gen2 paths in ADF. The notebook builds
   `abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{folder}` and leaves
   `STORAGE_ACCOUNT = "TODO_set_me"`. **Set the real account** (or set `USE_ONELAKE = True`
   and land the data in the attached lakehouse). Wire credentials via workspace identity /
   OneLake shortcut / `mssparkutils.credentials.getSecret(...)` — do not hardcode keys.

2. **[Schema drift]** Both sources have `allowSchemaDrift: true`. Spark has no runtime drift,
   so the notebook reads **all** columns (it does not pin the `output()` projection). This is
   safe for the downstream explicit `select`s, but confirm no late-arriving columns are
   required beyond those projected in `cityselect` / `salesselect`.

3. **[Join type & duplicate column]** `joinwithcity` is a **left** join (`how="left"`). Both
   inputs expose `CityKey`; the sides are aliased `s` / `c`, and the downstream `joinselect`
   drops `CityKey` by not selecting it (mirrors the MDF). Verify the row count against the
   original and confirm the intended side is kept. A left join can legitimately produce **null
   City/State** for sales whose `CityKey` has no matching (Texas/California) city — the
   validation cell reports that count.

4. **[Decimal precision]** `UnitPrice`/`TotalExcludingTax`/`TaxAmount`/`Profit`/`TotalIncludingTax`
   are `decimal(18,2)` and `TaxRate` is `decimal(18,3)`. No arithmetic is performed on them
   here (only rename/pass-through), so scale should be preserved — but confirm the sink schema
   matches via the validation cell if any upstream schema evolves.

5. **[Write mode]** The `sink` declared no explicit write behavior, so the notebook uses
   `mode("overwrite")` (recreate the target folder — the common ADF default). If the original
   sink appended, change to `mode("append")`. `umask`, `preCommands`, `postCommands`,
   `skipDuplicateMapInputs/Outputs` were ADF staging options with no Spark equivalent and were
   intentionally dropped.

---

## 5. Validation performed

The generated notebook includes a validation cell that:

| Check | What it does |
|---|---|
| Row counts (source → sink) | Prints counts for `dimcity`, `sales`, `salesfilter`, `cityfilter`, `cityselect`, `salesselect`, `joinselect` |
| Schema parity | Asserts the final 16 columns match the MDF projection **by name, type, and order** (`InvoiceDate`, `DeliveryDate`, `Quantity`, `UnitPrice` `decimal(18,2)`, `TaxRate` `decimal(18,3)`, `TotalWithoutTax`, `TaxAmount`, `Profit`, `TotalWithTax`, `City`, `State`, `Country`, `Continent`, `SalesTerritory`, `Region`, `Subregion`) |
| Null/key integrity | Reports rows with null `City` (expected left-join misses) |

These run against live data at execution time. For strongest parity, diff `joinselect`
against the original MDF sink output with `exceptAll` (see
`references/validation-and-testing.md`).

---

## 6. How to run in Fabric

1. Import `WideWorldImportersDW_City_Sales.ipynb` into your Fabric workspace.
2. Attach the target lakehouse (or set `USE_ONELAKE` / `STORAGE_ACCOUNT` in the parameters cell).
3. Select the parameters cell → **Toggle parameter cell** so pipeline/job overrides inject there.
4. Set parameter defaults or pass overrides from a pipeline Notebook activity.
5. Run all cells; review the validation cell output.

---

## 7. Known limitations / not converted

- `STORAGE_ACCOUNT` is a required `TODO` placeholder (path/account not present in the DFS).
- ADF staging/formatting options (`umask`, `preCommands`, `postCommands`,
  `skipDuplicateMap*`, `ignoreNoFilesFound`, `validateSchema`) are not represented — they have
  no Fabric Spark equivalent for this flow.
- The Papermill `parameters` cell tag is not embedded (VS Code serialization); toggle it in
  Fabric as noted in section 2.
