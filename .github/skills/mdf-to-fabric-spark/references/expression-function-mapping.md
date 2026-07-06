# MDF Expression Language → PySpark / Spark SQL Mapping

The MDF expression language appears inside every transformation (filters, join conditions,
derived columns, aggregations). Translate it to `pyspark.sql.functions` (aliased `F`) or,
when clearer, to a Spark SQL string passed to `F.expr("...")`.

Two valid strategies — pick per expression for readability:
1. **DataFrame API**: `F.upper(F.col("title"))`
2. **SQL expression**: `F.expr("upper(title)")` — often the most faithful for complex nested MDF expressions, since Spark SQL syntax is close to MDF.

---

## Operators

| MDF | PySpark API | Spark SQL |
|---|---|---|
| `&&` | `&` | `AND` |
| `\|\|` | `\|` | `OR` |
| `!` (not) | `~` | `NOT` |
| `==` | `==` | `=` |
| `!=` | `!=` | `<>` / `!=` |
| `<`,`>`,`<=`,`>=` | same | same |
| `+` (concat/add) | `+` (numbers) / `F.concat` (strings) | `+` / `\|\|` or `concat` |
| `-`,`*`,`/`,`%` | same / `F.pmod` for `%` | same |

When using the API with bitwise operators, wrap every comparison in parentheses:
`(F.col("a") > 1) & (F.col("b") < 2)`.

---

## Conditional / null handling

| MDF | PySpark |
|---|---|
| `iif(cond, t, f)` | `F.when(cond, t).otherwise(f)` |
| `iifNull(a, b)` / `coalesce(a, b, ...)` | `F.coalesce(a, b, ...)` |
| `isNull(x)` | `F.col("x").isNull()` |
| `isNotNull(x)` / `!isNull(x)` | `F.col("x").isNotNull()` |
| `case(c1, v1, c2, v2, default)` | chained `F.when(c1, v1).when(c2, v2).otherwise(default)` |
| `equals(a, b)` | `a == b` (null-unsafe) / `a.eqNullSafe(b)` |
| `nullIf` | `F.when(a == b, None).otherwise(a)` |

---

## String functions

| MDF | PySpark |
|---|---|
| `concat(a, b, ...)` / `a + b` | `F.concat(a, b, ...)` |
| `concatWS(sep, ...)` | `F.concat_ws(sep, ...)` |
| `upper` / `lower` / `initCap` | `F.upper` / `F.lower` / `F.initcap` |
| `trim` / `ltrim` / `rtrim` | `F.trim` / `F.ltrim` / `F.rtrim` |
| `length` | `F.length` |
| `substring(s, start, len)` | `F.substring(s, start, len)` (MDF is 1-based; Spark `substring` is 1-based too) |
| `left(s, n)` / `right(s, n)` | `F.substring(s, 1, n)` / `F.expr("right(s, n)")` |
| `replace(s, find, repl)` | `F.regexp_replace` (literal) or `F.replace` |
| `regexReplace(s, pat, repl)` | `F.regexp_replace(s, pat, repl)` |
| `regexExtract(s, pat, grp)` | `F.regexp_extract(s, pat, grp)` |
| `regexMatch(s, pat)` | `F.col("s").rlike(pat)` |
| `split(s, delim)` | `F.split(s, delim)` |
| `instr(s, sub)` / `locate` | `F.instr(s, sub)` / `F.locate` |
| `lpad` / `rpad` | `F.lpad` / `F.rpad` |
| `reverse` | `F.reverse` |
| `translate(s, from, to)` | `F.translate` |
| `startsWith` / `endsWith` | `F.col("s").startswith(...)` / `.endswith(...)` |
| `soundex` | `F.soundex` |
| `dropLeft/dropRight` | `F.expr("substring(...)")` |
| `substringIndex` | `F.substring_index` |

---

## Numeric / math

| MDF | PySpark |
|---|---|
| `round(x, d)` / `round(x)` | `F.round(x, d)` |
| `floor` / `ceil` | `F.floor` / `F.ceil` |
| `abs` | `F.abs` |
| `power(b, e)` | `F.pow(b, e)` |
| `sqrt`, `exp`, `log`, `log10` | `F.sqrt`, `F.exp`, `F.log`, `F.log10` |
| `mod(a, b)` / `a % b` | `F.pmod(a, b)` (or `%`) |
| `random(seed)` | `F.rand(seed)` |
| `least` / `greatest` | `F.least` / `F.greatest` |
| `sign` | `F.signum` |
| `trigonometric (sin,cos,...)` | `F.sin`, `F.cos`, ... |

---

## Type conversion

| MDF | PySpark |
|---|---|
| `toInteger(x)` | `F.col("x").cast("int")` |
| `toLong(x)` | `.cast("long")` |
| `toShort` / `toByte` | `.cast("short")` / `.cast("byte")` |
| `toDouble` / `toFloat` | `.cast("double")` / `.cast("float")` |
| `toDecimal(x, p, s)` | `.cast(DecimalType(p, s))` |
| `toString(x)` | `.cast("string")` |
| `toBoolean(x)` | `.cast("boolean")` |
| `toDate(s, 'format')` | `F.to_date(s, 'format')` |
| `toTimestamp(s, 'format')` | `F.to_timestamp(s, 'format')` |
| `typeMatch(type, ...)` | schema-driven logic (see rule-based below) |

**Format strings differ.** MDF uses Java `SimpleDateFormat`-style patterns (`yyyy-MM-dd HH:mm:ss`). Spark uses the Datetime pattern (mostly compatible, but confirm `mm` minutes vs `MM` months, and set `spark.sql.legacy.timeParserPolicy` if legacy patterns are needed). Flag format strings for review.

---

## Date / time

| MDF | PySpark |
|---|---|
| `currentTimestamp()` / `currentUTC()` | `F.current_timestamp()` |
| `currentDate()` | `F.current_date()` |
| `year`/`month`/`dayOfMonth`/`hour`/`minute`/`second` | `F.year`/`F.month`/`F.dayofmonth`/`F.hour`/`F.minute`/`F.second` |
| `dayOfWeek` / `dayOfYear` / `weekOfYear` | `F.dayofweek` / `F.dayofyear` / `F.weekofyear` |
| `addDays(d, n)` | `F.date_add(d, n)` |
| `addMonths(d, n)` | `F.add_months(d, n)` |
| `subDays` | `F.date_sub` |
| `dayDiff` / `monthsBetween` | `F.datediff` / `F.months_between` |
| `addHours/addMinutes/addSeconds` | interval arithmetic: `col + F.expr("INTERVAL n HOURS")` |
| `toUTC(ts, tz)` / `fromUTC` | `F.to_utc_timestamp` / `F.from_utc_timestamp` |
| `truncateTime` / date truncation | `F.date_trunc('day', ts)` / `F.trunc` |
| `unixTimestamp` | `F.unix_timestamp` |

---

## Hashing / crypto

| MDF | PySpark |
|---|---|
| `md5(...)` | `F.md5(F.concat_ws('', ...))` |
| `sha1(...)` | `F.sha1(...)` — for multiple cols: `F.sha1(F.concat_ws('', c1, c2, ...))` |
| `sha2(bits, ...)` | `F.sha2(F.concat_ws('', ...), bits)` |
| `crc32(...)` | `F.crc32(...)` |

MDF `sha1(colA, colB, colC)` hashes the concatenation. In Spark, concatenate first
(`F.concat_ws('||', "colA", "colB", "colC")`) to get a deterministic, collision-resistant
input, then hash. Note the concatenation strategy in the report so hashes are reproducible.

---

## Aggregate functions (inside `aggregate` / `window`)

| MDF | PySpark |
|---|---|
| `sum` / `avg` / `min` / `max` | `F.sum` / `F.avg` / `F.min` / `F.max` |
| `count()` | `F.count(F.lit(1))` |
| `count(col)` | `F.count("col")` (non-null count) |
| `countDistinct(col)` | `F.countDistinct("col")` |
| `countIf(cond)` | `F.sum(F.when(cond, 1).otherwise(0))` |
| `first(col)` / `last(col)` | `F.first("col", ignorenulls=True)` / `F.last(...)` |
| `collect(col)` | `F.collect_list("col")` (or `collect_set` for distinct) |
| `stddev` / `variance` | `F.stddev` / `F.variance` |
| `stddevPopulation`/`variancePopulation` | `F.stddev_pop` / `F.var_pop` |
| `kurtosis` / `skewness` | `F.kurtosis` / `F.skewness` |
| `string_agg` (via collect + toString) | `F.concat_ws(sep, F.collect_list(...))` |

AlterRow status predicates (inside aggregate after an alterRow): `isInsert()`, `isUpdate()`,
`isUpsert()`, `isDelete()` — in Spark these correspond to the tag column you created in the
AlterRow step; count them with `F.sum(F.when(F.col("_op") == 'delete', 1).otherwise(0))`.

---

## Array / map / complex

| MDF | PySpark |
|---|---|
| `array(a, b, ...)` | `F.array(...)` |
| `size(arr)` | `F.size(arr)` |
| `contains(arr, expr)` | `F.array_contains` / `F.exists(arr, lambda x: ...)` |
| `filter(arr, pred)` | `F.filter(arr, lambda x: ...)` |
| `map(arr, expr)` / `mapIndex` | `F.transform(arr, lambda x: ...)` |
| `reduce(arr, ...)` | `F.aggregate(arr, start, merge)` |
| `sort(arr)` | `F.array_sort` / `F.sort_array` |
| `slice` | `F.slice` |
| `flatten` | `F.flatten` |
| `at(arr, i)` / `arr[i]` | `F.element_at(arr, i)` (1-based) |
| `#item` (current array element) | the `lambda x:` bound variable in higher-order funcs |
| `#index` | `F.transform(arr, lambda x, i: ...)` second arg |
| `mapEntries` / `keyValues` | `F.map_entries` / `F.map_keys`,`F.map_values` |

---

## Column-set / schema-driven constructs (`$$`, `name`, `type`, `match`, `columns()`, `byName`)

These evaluate against the **runtime schema**, so translate them to Python that iterates the
DataFrame's schema at build time. **Always flag for review** because behavior depends on the
actual columns present (especially with schema drift).

| MDF token | Meaning | Spark handling |
|---|---|---|
| `columns()` | list of all column names | `df.columns` |
| `$$` | current column's value (in `each`) | `F.col(c)` while looping `for c in df.columns` |
| `name` | current column's name | the loop variable `c` |
| `type` | current column's data type | `dict(df.dtypes)[c]` |
| `#item` | current item in an array/each context | lambda bound var |
| `byName('col')` | value of a column by name | `F.col('col')` |
| `byNames(['a','b'])` | array of columns by name | `[F.col(x) for x in ['a','b']]` |
| `hasColumn('c')` | column existence test | `'c' in df.columns` |
| `match(predicate)` | filter columns by predicate | list comprehension over schema |
| `each(match(pred), $$ = expr)` | apply expr to matching columns | loop + `withColumn` |

### Rule-based `each`/`match` patterns

```
derive(each(match(type=='string'), $$ = upper($$))) ~> out
```

```python
out = in1
for c, t in in1.dtypes:
    if t == "string":
        out = out.withColumn(c, F.upper(F.col(c)))
```

```
aggregate(each(match(true()), $$ = countDistinct($$))) ~> out
```

```python
out = in1.agg(*[F.countDistinct(c).alias(c) for c in in1.columns])
```

```
select(mapColumn(each(match(true())))) ~> automap   # keep everything
```

```python
automap = in1   # identity
```

```
derive(DWhash = sha2(256, columns())) ~> out
```

```python
out = in1.withColumn("DWhash", F.sha2(F.concat_ws("||", *in1.columns), 256))
```

Type predicate values map as: `'string'`→`string`, `'integer'`→`int`, `'short'`→`smallint`,
`'long'`→`bigint`, `'double'`→`double`, `'float'`→`float`, `'decimal'`→`decimal(...)`,
`'boolean'`→`boolean`, `'date'`→`date`, `'timestamp'`→`timestamp`, `'complex'`→`struct`,
`'array'`→`array`. Compare against `df.dtypes` values (which use Spark's SQL type spellings).

---

## Parameters and locals

- `$ParamName` → substitute the notebook parameter variable `ParamName` (a Python value). In SQL expressions, inline the literal or use an f-string.
- `:LocalName` (data flow locals / cached) → precompute a Python variable or a small DataFrame and reference it.

## When in doubt

If an MDF expression has no clean API equivalent, wrap the near-identical SQL text in
`F.expr("...")`. Spark SQL's function names and semantics are close to MDF's, so this is
often the fastest faithful translation — then verify against sample data.
