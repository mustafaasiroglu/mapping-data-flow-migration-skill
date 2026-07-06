# Validation & Testing the Conversion

How to give the user confidence that the converted notebook produces the same result as the
original mapping data flow. Add a validation cell to the notebook and follow this checklist.

---

## 1. Static review (before running)

- [ ] Every MDF stream has a corresponding DataFrame variable of the same name.
- [ ] The DAG order is preserved (every input is defined before it is used).
- [ ] Every `source` maps to a read; every `sink` maps to a write.
- [ ] Branch points (a stream feeding two transforms) reuse the same variable.
- [ ] Multi-input transforms (join/union/lookup/exists) reference the correct two streams.
- [ ] Column projection/order at each `select`/`sink` matches the MDF `mapColumn`/`output`.
- [ ] Every expression was translated (no leftover MDF-only syntax like `$$`, `iif`, `~>`).
- [ ] Data types at the sink match the MDF `output()` schema.

---

## 2. Schema parity check

Compare the final DataFrame schema against the MDF sink's expected columns/types:

```python
expected = [
    ("InvoiceDate", "timestamp"),
    ("Quantity", "int"),
    ("TotalWithoutTax", "decimal(18,2)"),
    ("City", "string"),
    # ... from the sink's mapColumn / output()
]
actual = [(f.name, f.dataType.simpleString()) for f in final_df.schema.fields]
print("Actual:", actual)
missing = [c for c, _ in expected if c not in [a for a, _ in actual]]
extra   = [a for a, _ in actual if a not in [c for c, _ in expected]]
assert not missing, f"Missing columns: {missing}"
assert not extra,   f"Unexpected columns: {extra}"
```

---

## 3. Row-count sanity checks

```python
print("source rows:", dimcity.count(), sales.count())
print("after filter:", salesfilter.count())
print("final rows:", final_df.count())
```

Compare against the MDF debug "data preview" row counts or a known production run. Large
discrepancies usually point to a join type mismatch (inner vs left) or an `&&`/`||` operator
translation error.

---

## 4. Null / key integrity checks

```python
# Keys that should never be null after a left join:
assert final_df.filter(F.col("CityKey").isNull()).count() == 0, "Unexpected null CityKey"

# Duplicate check where the MDF flow guarantees uniqueness:
dupes = final_df.groupBy("SaleKey").count().filter("count > 1").count()
assert dupes == 0, f"{dupes} duplicate keys"
```

---

## 5. Value-level comparison (strongest)

If the original data flow's output is available (or can be regenerated), diff the two:

```python
mdf_output = spark.read.format("parquet").load(mdf_output_path)   # original result
converted  = final_df

# Rows in one but not the other (both directions):
only_in_converted = converted.exceptAll(mdf_output)
only_in_mdf       = mdf_output.exceptAll(converted)
print("only_in_converted:", only_in_converted.count())
print("only_in_mdf:", only_in_mdf.count())
# Both should be 0 for exact parity. Inspect samples if not:
only_in_converted.show(20, truncate=False)
```

Tips:
- Align column order/types before `exceptAll` (cast to the same schema).
- For floating-point/decimal columns, round to a tolerance before comparing.
- For non-deterministic columns (surrogate keys, `current_timestamp`, `monotonically_increasing_id`), exclude them from the diff and validate separately.

---

## 6. Common failure modes & fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| Too many rows after join | `inner` used where MDF was `left`, or duplicate right-side keys | check `joinType`; dedupe lookup dim with a window |
| Fewer rows than expected | `&&`/`\|\|` swapped, or wrong operator precedence | parenthesize each comparison; re-read filter |
| Null columns after join | ambiguous column resolved to wrong side | use `left["col"]`/`right["col"]`, mirror the MDF `select` |
| Wrong decimal scale | Spark arithmetic rescaling | explicit `.cast(DecimalType(p, s))` before sink |
| Timestamp parse errors / nulls | MDF vs Spark format pattern mismatch | fix the pattern; last resort `LEGACY` time parser |
| Non-sequential surrogate keys | `monotonically_increasing_id()` | use `row_number().over(window)` + startAt offset |
| Order lost at sink | shuffle after `sort` | `orderBy` right before the write |
| Missing columns with schema drift | pinned projection on a drifting source | read all columns; `unionByName(allowMissingColumns=True)` |

---

## 7. What to record in the report

For each check you ran: pass/fail, counts, and any residual differences. List every
transformation flagged for manual review (see the SKILL.md "human review" list) with a clear
explanation of the risk and what the user should verify.
