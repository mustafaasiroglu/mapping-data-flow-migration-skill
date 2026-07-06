# Conversion Report — <DATA_FLOW_NAME>

**Source DFS:** `<path-or-artifact>`
**Generated notebook:** `<notebook-file>.ipynb`
**Target runtime:** Microsoft Fabric Spark (PySpark)
**Date:** <yyyy-mm-dd>

---

## 1. Summary

| Metric | Value |
|---|---|
| Total transformations | <n> |
| Sources | <n> |
| Sinks | <n> |
| Joins / lookups / unions | <n> |
| Manual-review items | <n> |
| Overall confidence | High / Medium / Low |

Short prose summary of what the data flow does end to end.

---

## 2. Parameters mapped

| MDF parameter | Type | Default | Notebook variable |
|---|---|---|---|
| `Container` | string | `data` | `Container` |
| ... | ... | ... | ... |

Storage/Fabric config that the user must set: `STORAGE_ACCOUNT`, lakehouse attachment, etc.

---

## 3. Transformation-by-transformation mapping

| # | MDF stream | Type | PySpark mapping | Notes |
|---|---|---|---|---|
| 1 | `dimcity` | source | `spark.read.parquet(...)` | pinned to `output()` schema |
| 2 | `sales` | source | `spark.read.parquet(...)` | |
| 3 | `salesfilter` | filter | `.filter((col>700) & (col>500))` | |
| ... | ... | ... | ... | ... |

---

## 4. Manual-review items (⚠️ verify these)

List each item with the risk and the exact thing to check. Examples:

- **[Join type]** `joinwithcity` uses `joinType:'left'` → mapped to `how="left"`. Verify row counts vs. the original; confirm the duplicate `CityKey` column is resolved to the intended side.
- **[Schema drift]** `allowSchemaDrift: true` on `dimcity`/`sales`. Spark has no runtime drift; the notebook reads the declared columns only. Confirm no late-arriving columns are expected.
- **[Data types]** `decimal(18,2)`/`decimal(18,3)` columns — verify Spark arithmetic didn't change the scale before the sink.
- **[Surrogate keys]** (if any) `monotonically_increasing_id()` is not gap-free — switch to `row_number()` if sequential keys are required.
- **[AlterRow/MERGE]** (if any) confirm key columns and policy order for the Delta `MERGE`.
- **[Timestamp formats]** verify `toTimestamp`/`toDate` patterns parse correctly under the Spark datetime parser.
- **[Sort/order]** ordering is not preserved across shuffles; re-sort before the sink if order matters.

---

## 5. Validation performed

| Check | Result |
|---|---|
| Schema parity | pass/fail (details) |
| Row counts (source → sink) | numbers |
| Null/key integrity | pass/fail |
| Value diff vs. original output (if available) | `exceptAll` counts |

---

## 6. How to run in Fabric

1. Import `<notebook-file>.ipynb` into your Fabric workspace.
2. Attach the target lakehouse (or set `USE_ONELAKE`/`STORAGE_ACCOUNT` in the parameters cell).
3. Set parameter defaults or pass overrides from a pipeline Notebook activity.
4. Run all cells; review the validation cell output.

---

## 7. Known limitations / not converted

List anything intentionally left as `TODO` (unknown paths, unsupported/custom transforms,
external calls) so the user knows what still needs wiring up.
