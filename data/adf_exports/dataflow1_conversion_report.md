# Conversion Report — dataflow1

**Source DFS:** `adf_exports/dataflow1.txt`
**Generated notebook:** `adf_exports/dataflow1.ipynb`
**Target runtime:** Microsoft Fabric Spark (PySpark)
**Date:** 2026-07-07

---

## 1. Summary

| Metric | Value |
|---|---|
| Total transformations | 2 |
| Sources | 1 |
| Sinks | 1 |
| Joins / lookups / unions | 0 |
| Manual-review items | 4 |
| Overall confidence | Medium |

This data flow is a straight **passthrough copy**: it reads a single file (`manifest.json`)
as *delimited (CSV)* text from the `msexports` container and writes the rows, unchanged, to a
sink. There are no transformations (no select/filter/derive/join) between source and sink.
Two aspects require confirmation: the source reads a JSON file *as delimited text*, and the
sink's format/destination is not present in the DFS.

---

## 2. Parameters mapped

The MDF script has **no `parameters{}` block**. The literal dataset properties were exposed
as notebook variables (tagged `parameters` cell) so the notebook is configurable and
overridable from a Fabric pipeline.

| Notebook variable | Source in DFS | Value |
|---|---|---|
| `SOURCE_CONTAINER` | `fileSystem` | `msexports` |
| `SOURCE_FOLDER_PATH` | `folderPath` | `focuscost/focus/must-focus-cost/20260401-20260430/8b451d18-4a38-4be1-b8a9-64fa666d531d` |
| `SOURCE_FILE_NAME` | `fileName` | `manifest.json` |
| `COLUMN_DELIMITER` | `columnDelimiter` | `,` |
| `QUOTE_CHAR` | `quoteChar` | `"` |
| `ESCAPE_CHAR` | `escapeChar` | `\` |
| `COLUMN_NAMES_AS_HEADER` | `columnNamesAsHeader` | `False` |
| `SINK_FORMAT` / `SINK_CONTAINER` / `SINK_PATH` / `SINK_WRITE_MODE` | *(not in DFS)* | **TODO** |

Storage/Fabric config the user must set: `STORAGE_ACCOUNT` (ADLS Gen2 account) or set
`USE_ONELAKE = True` and attach the target lakehouse.

---

## 3. Transformation-by-transformation mapping

| # | MDF stream | Type | PySpark mapping | Notes |
|---|---|---|---|---|
| 1 | `source1` | source | `spark.read.format("csv").option(...).load(.../manifest.json)` | delimited read; `header=False`, `sep=','`, `quote='"'`, `escape='\'`; no `output()` schema → all columns string (`_c0`, `_c1`, …) |
| 2 | `sink1` | sink | `source1.write.format(SINK_FORMAT).mode(SINK_WRITE_MODE)...` | destination not in DFS → `SINK_*` placeholders; guarded to fail loudly until set |

---

## 4. Manual-review items (⚠️ verify these)

- **[Source format vs. file type]** The MDF reads `manifest.json` with `format: 'delimited'`
  and `columnNamesAsHeader: false`. This parses a JSON file as **comma-delimited text**, not
  as JSON. Spark will emit positional columns `_c0`, `_c1`, … split wherever a `,` appears in
  the file. This mirrors the MDF behavior exactly, but confirm it is intended. If the goal is
  to read the JSON structure, switch to `spark.read.option("multiLine", True).json(path)`.
- **[Sink destination missing]** `sink1` has **no `format`, `fileSystem`, or `folderPath`** in
  the DFS (inline dataset or dataset-reference sink). The notebook exposes `SINK_FORMAT`,
  `SINK_CONTAINER`, `SINK_PATH`, and `SINK_WRITE_MODE` as **TODO** parameters and raises a
  `ValueError` if they are left unset. Set them to the original sink target (Delta table,
  parquet folder, etc.) before running.
- **[Schema drift]** `allowSchemaDrift: true` on both source and sink. Spark has no runtime
  drift; the notebook reads whatever columns the delimited parse yields. Confirm no
  late-arriving/variable columns need special handling.
- **[ignoreNoFilesFound: false]** The MDF fails if the path is missing; the default Spark read
  matches this (it errors when the file/path does not exist). No extra guard was added.

---

## 5. Validation performed

| Check | Result |
|---|---|
| Schema parity | N/A — no `output()` schema in the DFS; positional columns produced by the delimited parse |
| Row counts (source → sink) | Not run (no live cluster / storage in this conversion). Validation cell prints `source1.count()` and schema. |
| Null/key integrity | N/A — passthrough, no keys |
| Value diff vs. original output | Not available — run after wiring up `SINK_*` and `STORAGE_ACCOUNT` |

---

## 6. How to run in Fabric

1. Import `adf_exports/dataflow1.ipynb` into your Fabric workspace.
2. Attach the target lakehouse (or set `USE_ONELAKE` / `STORAGE_ACCOUNT` in the parameters cell).
3. Set the `SINK_*` parameters to the intended destination (they are `TODO_set_me` by default).
4. Run all cells; review the validation cell output.

---

## 7. Known limitations / not converted

- **Sink target** is not specified in the DFS and is left as `TODO` parameters.
- **`skipDuplicateMapInputs` / `skipDuplicateMapOutputs`** on the sink are MDF mapping de-dup
  flags with no Spark equivalent; safely dropped (no column mapping is present).
- **`useSchema: false` / `validateSchema: false`** — no schema is pinned or validated, matching
  the source's inferred/positional behavior.
