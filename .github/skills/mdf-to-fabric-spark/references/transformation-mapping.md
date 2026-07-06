# MDF Transformation → PySpark Mapping

The complete catalog of mapping-data-flow transformations and their PySpark (DataFrame API)
equivalents. Assume `from pyspark.sql import functions as F, Window` and `from pyspark.sql.types import *`.
Each MDF stream becomes a DataFrame variable **of the same name**.

> Convention below: `in1`, `in2` are input stream names; `out` is the output stream name.
> Replace with the real names from the DFS.

---

## Sources & sinks

### `source` → read

```
source(output(colA as string, colB as integer, ...),
  allowSchemaDrift: true, validateSchema: false,
  format: 'parquet', fileSystem: ($Container), folderPath: ($Path)) ~> src
```

```python
# path built from container + folderPath (see fabric-notebook-guide.md)
src = spark.read.format("parquet").load(f"{base_path}/{folder_path}")
# If validateSchema or you must pin the projection, apply the output() schema explicitly:
src = src.select(
    F.col("colA").cast("string").alias("colA"),
    F.col("colB").cast("int").alias("colB"),
)
```

- `format`: `parquet|delta|csv|json|avro|orc`. For `delta` use `spark.read.format("delta").load(path)` or `spark.read.table("lakehouse.table")`.
- CSV/JSON: carry over `columnDelimiter`, `firstRowAsHeader` (`header=True`), `quoteChar`, `escapeChar`, `nullValue`, `multiLineRow` (`multiLine=True`).
- Inline dataset vs. dataset reference: either way, resolve to a concrete path/table for Fabric.
- `wildcardPaths` → pass a glob or list of paths to `.load([...])`.

### `sink` → write

```
joinselect sink(allowSchemaDrift: true, format: 'parquet',
  partitionBy: ..., fileSystem: ($SinkContainer), folderPath: ($SinkPath)) ~> snk
```

```python
(out.write
    .format("parquet")               # or "delta"
    .mode("overwrite")               # map from sink's write behavior
    .save(f"{sink_base}/{sink_folder}"))
# Delta table sink:
out.write.format("delta").mode("overwrite").saveAsTable("lakehouse.MyTable")
```

- Write mode mapping: recreate/overwrite → `overwrite`; append → `append`; truncate → `overwrite` (or `TRUNCATE` then append); error if exists → `errorifexists`; ignore → `ignore`.
- `partitionBy: (colA, colB)` → `.partitionBy("colA", "colB")`.
- **AlterRow upstream** (upsert/update/delete policies) → Delta `MERGE`, see AlterRow below.
- File name control (`filePattern`, single file "output to single file") → coalesce(1) + rename, note perf cost.

---

## Schema / column transforms

### `select` (mapColumn) → `select` / rename

```
in1 select(mapColumn(
    CityKey,
    State = StateProvince,
    Country
  ), skipDuplicateMapInputs: true, skipDuplicateMapOutputs: true) ~> out
```

```python
out = in1.select(
    F.col("CityKey"),
    F.col("StateProvince").alias("State"),
    F.col("Country"),
)
```

- Right-hand side may be any expression, not just a column → build with `F.expr(...)` or function calls.
- Rule-based `select(mapColumn(each(match(true()))))` = keep all columns → `out = in1` (identity) or explicit re-projection.

### `derive` (derived column) → `withColumn`

```
in1 derive(upperTitle = upper(title), Year = year(InvoiceDate)) ~> out
```

```python
out = (in1
    .withColumn("upperTitle", F.upper(F.col("title")))
    .withColumn("Year", F.year(F.col("InvoiceDate"))))
```

- Rule-based derive (`each(match(type=='string'), $$ = trim($$))`) → iterate schema:

```python
out = in1
for c, t in in1.dtypes:
    if t == "string":
        out = out.withColumn(c, F.trim(F.col(c)))
```

### `cast` → `cast`

```
in1 cast(output(Amount as decimal(18,2), Ts as timestamp), errors: true) ~> out
```

```python
out = in1.select(
    *[c for c in in1.columns if c not in ("Amount", "Ts")],
    F.col("Amount").cast(DecimalType(18, 2)).alias("Amount"),
    F.col("Ts").cast("timestamp").alias("Ts"),
)
```

- `errors: true` adds error columns in MDF; in Spark, invalid casts yield `null` — add a validation check if error tracking is required.

### `stringify` → build string column from complex type

```python
out = in1.withColumn("json_col", F.to_json(F.col("struct_col")))
```

---

## Row transforms

### `filter` → `filter` / `where`

```
in1 filter(TotalExcludingTax > 700 && Profit > 500) ~> out
```

```python
out = in1.filter((F.col("TotalExcludingTax") > 700) & (F.col("Profit") > 500))
# or, mirroring the expression directly:
out = in1.filter("TotalExcludingTax > 700 AND Profit > 500")
```

- `&&`→`&` / `AND`, `||`→`|` / `OR`, `!`→`~` / `NOT`. Parenthesize each comparison when using bitwise operators.

### `alterRow` → tag operations, then Delta `MERGE`

```
in1 alterRow(upsertIf(isNull(deleted)), deleteIf(deleted == 1)) ~> out
```

AlterRow only *tags* rows; the sink applies them. Map to a Delta merge at the sink:

```python
from delta.tables import DeltaTable
tgt = DeltaTable.forName(spark, "lakehouse.Target")
(tgt.alias("t")
    .merge(out.alias("s"), "t.KeyCol = s.KeyCol")
    .whenMatchedDelete(condition="s.deleted = 1")
    .whenMatchedUpdateAll(condition="s.deleted IS NULL")
    .whenNotMatchedInsertAll(condition="s.deleted IS NULL")
    .execute())
```

- Confirm **key columns** (from the sink's key settings) and **policy order** with the user. Policies: `insertIf`, `updateIf`, `deleteIf`, `upsertIf`.

### `assert` → validation, then raise

```python
bad = in1.filter(~(F.col("Amount") >= 0))
if bad.limit(1).count() > 0:
    raise ValueError("Assert failed: Amount must be >= 0")
out = in1  # pass-through when assertions are non-blocking; else filter/flag
```

---

## Multi-stream transforms

### `join` → `join`

```
left, right join((left@CityKey) == (right@CityKey),
  joinType: 'left', broadcast: 'auto') ~> out
```

```python
out = left.join(F.broadcast(right), left.CityKey == right.CityKey, how="left")
```

- `joinType`: `inner|left|right|full|cross`. In PySpark `how`: `inner|left|right|outer|cross`.
- `broadcast: 'left'|'right'|'both'|'auto'|'off'` → wrap the chosen side in `F.broadcast(...)`; `off`/`auto` → omit.
- **Duplicate column names**: after the join both `CityKey`s exist. Resolve immediately (drop one, or use a `select` mirroring the downstream MDF `select`). Use `left["CityKey"]` / `right["CityKey"]` to disambiguate, matching `stream@column`.
- `matchType: 'exact'` is standard equality; custom conditions become boolean `F.expr`.

### `lookup` → left join (+ optional multiple-match handling)

```
in1, dim lookup(in1@Id == dim@Id, multiple: false, pickup: 'any') ~> out
```

```python
out = in1.join(F.broadcast(dim), in1.Id == dim.Id, how="left")
# multiple:false + pickup:'first'/'last' → dedupe dim first with a Window:
w = Window.partitionBy("Id").orderBy(F.col("SortCol").asc())   # or desc for 'last'
dim1 = dim.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
out = in1.join(F.broadcast(dim1), in1.Id == dim1.Id, how="left")
```

- Lookup always keeps all left rows (like left join) and adds matched right columns.

### `exists` → semi / anti join

```
in1, in2 exists(in1@Key == in2@Key, negate: false, broadcast: 'auto') ~> out
```

```python
out = in1.join(in2, in1.Key == in2.Key, how="leftsemi")     # negate:false
out = in1.join(in2, in1.Key == in2.Key, how="leftanti")     # negate:true
```

### `union` → `unionByName`

```
in1, in2 union() ~> out
```

```python
out = in1.unionByName(in2, allowMissingColumns=True)   # name-based (MDF default aligns by name/position)
```

- If MDF is configured to union **by position**, use `in1.union(in2)` and ensure identical column order/types.

---

## Split & routing

### `split` (conditional split) → filtered DataFrames

```
in1 split(Type == 'A', Type == 'B', disjoint: false) ~> Route@(a, b, others)
```

```python
cond_a = F.col("Type") == "A"
cond_b = F.col("Type") == "B"
Route_a = in1.filter(cond_a)
Route_b = in1.filter(cond_b)
Route_others = in1.filter(~(cond_a | cond_b))   # the default/else stream
```

- `disjoint: true` → each row goes to the **first** matching stream only: chain the exclusions (`cond_b & ~cond_a`, etc.).
- `disjoint: false` → a row can appear in multiple streams: use each condition independently.
- The last named output is the **default** (rows matching no condition).

### `new branch` → reuse the DataFrame

MDF "New branch" isn't a keyword — it appears as the same stream name feeding two
transforms. In PySpark just reference the same variable twice. Consider `.cache()` if the
branch is reused and expensive.

---

## Aggregation & windowing

### `aggregate` → `groupBy().agg()`

```
in1 aggregate(groupBy(Year), Total = sum(Amount), Cnt = count()) ~> out
```

```python
out = in1.groupBy("Year").agg(
    F.sum("Amount").alias("Total"),
    F.count(F.lit(1)).alias("Cnt"),
)
```

- No `groupBy` → aggregate over the whole DataFrame: `in1.agg(...)` (single row).
- Rule-based aggregate (`each(match(...), $$ = ...)`) → build the agg list dynamically from the schema (see expression reference for `each`/`match`).
- `countDistinct`, `collect` (→ `F.collect_list`), `first`, `stddev`, `variance`, `countIf` (→ `F.sum(F.when(cond,1).otherwise(0))` or `F.count(F.when(cond, True))`).

### `window` → Window functions

```
in1 window(over(stocksymbol), asc(Date, true),
  startRowOffset: -7L, endRowOffset: 7L,
  MovingAvg = round(avg(Close), 2)) ~> out
```

```python
w = (Window.partitionBy("stocksymbol").orderBy(F.col("Date").asc())
        .rowsBetween(-7, 7))
out = in1.withColumn("MovingAvg", F.round(F.avg("Close").over(w), 2))
```

- `over(...)` → `partitionBy`; `asc/desc(col, nullsFirst)` → `orderBy`.
- `startRowOffset/endRowOffset` → `rowsBetween`; range offsets → `rangeBetween`.
- Window aggregate functions: `lag`, `lead`, `rank`, `denseRank`, `rowNumber`, `nTile`, `cumeDist`, `first`, `last`, `sum`, `avg`, etc. → `F.lag`, `F.lead`, `F.rank`, `F.dense_rank`, `F.row_number`, `F.ntile`, ...

### `rank` → `row_number` / `rank` / `dense_rank`

```
in1 rank(desc(Score), dense: true, output(RankCol as long)) ~> out
```

```python
w = Window.orderBy(F.col("Score").desc())
out = in1.withColumn("RankCol", F.dense_rank().over(w))   # rank() if dense:false
```

- MDF rank can be `caseInsensitive`/dense and supports partitioning via multiple sort keys.

### `pivot` → `groupBy().pivot().agg()`

```
in1 pivot(groupBy(Country), pivotBy(Year, ['2019','2020']),
  Total = sum(Amount), columnNaming: '$N_$V') ~> out
```

```python
out = (in1.groupBy("Country")
         .pivot("Year", ["2019", "2020"])
         .agg(F.sum("Amount").alias("Total")))
```

- If `pivotBy` lists explicit values, pass them to `.pivot(col, values)` (faster, deterministic).
- `columnNaming` controls output column names; rename afterward to match (`$N`=agg name, `$V`=pivot value, `$$`=column).

### `unpivot` → `stack` / `melt`

```
in1 unpivot(...) ~> out
```

```python
out = in1.selectExpr(
    "Country",
    "stack(2, '2019', Y2019, '2020', Y2020) as (Year, Amount)"
).where("Amount is not null")
```

---

## Keys, ordering, hierarchy

### `keyGenerate` / `surrogateKey` → sequential key

```
in1 keyGenerate(output(sk as long), startAt: 1L) ~> out
```

```python
# Sequential & gap-free (mirrors MDF). Requires an ordering; use a stable one.
w = Window.orderBy(F.monotonically_increasing_id())
out = in1.withColumn("sk", F.row_number().over(w) + F.lit(START_AT - 1))
```

- `F.monotonically_increasing_id()` alone is **not** sequential/contiguous — only use it when gaps are acceptable. Flag this choice in the report.
- For very large data, `zipWithIndex` on the RDD is an alternative to a single-partition window.

### `sort` → `orderBy`

```
in1 sort(desc(Amount), asc(Name)) ~> out
```

```python
out = in1.orderBy(F.col("Amount").desc(), F.col("Name").asc())
```

- **Important:** ordering is not preserved across later shuffles (joins/aggregates). If order matters at the sink, sort **immediately before** the sink. Note this in the report.

### `flatten` → `explode`

```
in1 flatten(unroll(Items), Item = Items) ~> out
```

```python
out = in1.withColumn("Item", F.explode_outer(F.col("Items")))
# unrollRoot / nested selection → select the struct fields after exploding:
out = out.select("*", "Item.*")
```

- `explode` drops rows with empty/null arrays; `explode_outer` keeps them (matches MDF "include nulls").
- Multiple `unroll` columns → chain explodes or use `arrays_zip` + explode to keep them aligned.

### `parse` → `from_json` / `regexp` extraction

```
in1 parse(parsed = jsonCol ? (a as string, b as integer), format: 'json') ~> out
```

```python
schema = StructType([StructField("a", StringType()), StructField("b", IntegerType())])
out = in1.withColumn("parsed", F.from_json(F.col("jsonCol"), schema))
# then out.select("*", "parsed.*") to promote fields
```

- `format: 'json'` → `from_json`; delimited → `split`/`regexp_extract`; XML → `from_xml` (spark-xml) or a UDF.

---

## Modular / advanced

### `flowlet` → inline the reusable fragment

Flowlets are reusable data-flow fragments (like functions). There is no direct Spark
equivalent — **inline** the flowlet's transformations into the graph, wiring its `input`
to the calling stream and its `output` to the downstream stream. If reused many times,
extract a Python function that takes and returns a DataFrame.

### `externalCall` → UDF / REST call

Map to a UDF that calls the endpoint (use `requests` inside a `pandas_udf` for batching),
or pre-materialize the lookup. Flag as manual review — throughput/retry/auth differ.

### `sink cache` / cached lookup → `broadcast` or `.cache()`

A cached sink used as a lookup source elsewhere → compute that DataFrame once, `.cache()`
it, and `F.broadcast(...)` it into the join.

---

## Quick lookup table

| MDF transform | PySpark |
|---|---|
| `source` | `spark.read.format(...).load()` / `spark.read.table()` |
| `sink` | `df.write.format(...).mode(...).save()/saveAsTable()` |
| `select` / `mapColumn` | `df.select(...)`, `.alias()` |
| `derive` | `df.withColumn()` |
| `cast` | `.cast()` in a select |
| `filter` | `df.filter()/where()` |
| `join` | `df.join(other, cond, how)` |
| `lookup` | `df.join(dim, cond, "left")` (+ dedupe) |
| `exists` | `leftsemi` / `leftanti` join |
| `union` | `df.unionByName()` / `union()` |
| `split` (conditional) | multiple `df.filter()` |
| `aggregate` | `df.groupBy().agg()` |
| `window` | `Window` + `F.*().over(w)` |
| `rank` | `F.rank()/dense_rank()/row_number().over(w)` |
| `pivot` | `df.groupBy().pivot().agg()` |
| `unpivot` | `stack(...)` via `selectExpr` |
| `keyGenerate`/`surrogateKey` | `row_number().over(w)` (+ startAt) |
| `sort` | `df.orderBy()` |
| `alterRow` | tag → Delta `MERGE` at sink |
| `flatten` | `F.explode_outer()` |
| `parse` | `F.from_json()` / regex |
| `stringify` | `F.to_json()` |
| `assert` | filter + `raise` |
| `flowlet` | inline / Python function |
| `externalCall` | UDF / REST |
| `new branch` | reuse DataFrame variable |
