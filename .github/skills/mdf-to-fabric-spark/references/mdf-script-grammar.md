# MDF Script (DFS) Grammar Reference

How to reliably parse Data Flow Script so the conversion is correct. Read this before
splitting a flow into transformations.

## 1. Overall shape

A DFS file is (optionally) a `parameters{...}` block followed by a sequence of
transformation statements. Whitespace and newlines are insignificant except inside
string literals. The service also collapses the whole script to a single line for
API/PowerShell use — re-expand before parsing.

```
parameters{ <param> as <type> (<default>), ... }        # optional, at most once, first
<stmt>
<stmt>
...
```

Each statement (except sources) starts with one or more **input stream names**, then a
transformation call, then the stream terminator `~>` and the **output stream name**:

```
inputStream transformationType( body ) ~> outputStream
```

Sources have no input stream:

```
source( body ) ~> sourceName
```

## 2. Statement anatomy

| Part | Notes |
|------|-------|
| Input stream(s) | Zero (source), one (most), or many (join, union, lookup, exists). Multiple inputs are comma-separated: `left, right join(...)`. |
| Transformation type | Lowercase keyword: `source`, `sink`, `select`, `filter`, `derive`, `aggregate`, `join`, `lookup`, `exists`, `union`, `split`, `window`, `rank`, `keyGenerate`, `surrogateKey`, `sort`, `alterRow`, `pivot`, `unpivot`, `flatten`, `parse`, `cast`, `stringify`, `flowlet`, `assert`, `sink`. |
| Body | Comma-separated properties inside `( ... )`. May contain **nested** parentheses and nested calls (e.g. `output(...)`, `mapColumn(...)`, `each(match(...))`). Must be matched by balancing parentheses, **not** by the first `)`. |
| `~>` | Stream terminator. Always precedes the output name. |
| Output stream | The DataFrame name to produce. Must be unique. |

### Multi-output transformations

`split` (conditional split) and other row-routers can produce **named outputs** using the
`@(name1, name2)` suffix:

```
split(condition, disjoint: false) ~> LookForNULLs@(hasNULLs, noNULLs)
```

Here `LookForNULLs` is the transform; `hasNULLs` and `noNULLs` are two output DataFrames.
Downstream you reference them as `LookForNULLs@hasNULLs`.

## 3. Parsing algorithm (recommended)

1. Strip a leading `parameters{...}` block if present (balance the braces).
2. Scan the rest, splitting into statements. A statement ends at ` ~> <name>` where the
   `~>` is at **paren-depth 0**. (Track depth across `(` `)` and `[` `]`; ignore terminators inside string literals `'...'` and `"..."`.)
3. For each statement, from the left read whitespace-separated tokens until you hit the
   transformation keyword followed by `(` — those leading tokens (comma-separated) are the
   input streams.
4. Capture the transformation type, then the balanced `( ... )` body.
5. Capture the output name (and any `@(...)` split outputs) after `~>`.

The helper [../scripts/parse_mdf.py](../scripts/parse_mdf.py) implements exactly this and
emits the transformations in topological (source→sink) order.

## 4. The `output(...)` schema block

Sources (and some transforms) declare a projection with `output(...)`:

```
source(output(
    CityKey as integer,
    City as string,
    ValidFrom as timestamp,
    LatestRecordedPopulation as long,
    UnitPrice as decimal(18,2)
  ),
  allowSchemaDrift: true,
  validateSchema: false,
  format: 'parquet',
  fileSystem: ($Container),
  folderPath: ($CityFolderPath)) ~> dimcity
```

- Each entry is `ColumnName as type`. Preserve name and order.
- Use it to build an explicit Spark schema (`StructType`) or to `select`/`cast` after read.
- `decimal(p,s)` → `DecimalType(p, s)`; `short` → `ShortType`; `long` → `LongType`;
  `integer` → `IntegerType`; `string` → `StringType`; `timestamp` → `TimestampType`;
  `date` → `DateType`; `boolean` → `BooleanType`; `double` → `DoubleType`;
  `float` → `FloatType`. Complex types: `(...)` = struct, `[]` = array, `[... as ...]` = map.

## 5. Column mapping (`mapColumn`) syntax

`select` and `sink` map columns with `mapColumn(...)`:

```
select(mapColumn(
    CityKey,                     # keep as-is
    State = StateProvince,       # rename: newName = sourceExpr
    Country
  ),
  skipDuplicateMapInputs: true,
  skipDuplicateMapOutputs: true) ~> cityselect
```

- Bare name = passthrough.
- `New = Old` = rename / computed column (right side is an expression).
- `skipDuplicateMapInputs/Outputs` = de-dup behavior; usually safe to ignore in Spark but note if collisions exist.

### Rule-based mapping

`select`/`derive` can operate on many columns at once with `each` + `match`:

```
select(mapColumn(each(match(true()))), ...) ~> automap          # keep all columns
derive(each(match(type=='string'), $$ = trim($$))) ~> d1        # trim every string column
```

- `$$` = the current column's value; `name` = its name; `type` = its type.
- `columns()` = array of all column names in the stream.
- Translate these into dynamic Python that iterates `df.dtypes`/`df.schema` — see the expression-mapping reference.

## 6. Parameters and references

- `parameters{ Name as string ("default") }` declares flow parameters. Reference them as `$Name` anywhere in expressions/properties.
- `$$` and `#item` are contextual (current column / current array item in `each`/array funcs), **not** parameters.
- `stream@column` disambiguates a column when two joined inputs share a name: `salesselect@CityKey` vs `cityselect@CityKey`.
- Dataset properties commonly seen: `fileSystem`, `folderPath`, `fileName`, `format` (`parquet`, `delta`, `csv`, `json`, `avro`, `orc`), `wildcardPaths`, `partitionBy`.

## 7. Common source/sink properties (map or ignore)

| Property | Meaning | Spark handling |
|----------|---------|----------------|
| `allowSchemaDrift` | include unmodeled columns | read all columns; flag for review |
| `validateSchema` | fail if schema differs | optional `assert`/schema check |
| `ignoreNoFilesFound` | don't fail on empty path | wrap read / check path |
| `format` | file format | `spark.read.format(...)` |
| `fileSystem` / `folderPath` / `fileName` | ADLS container/path | build `abfss://` or lakehouse path |
| `partitionBy` | output partition columns | `.partitionBy(...)` on write |
| `umask`, `preCommands`, `postCommands` | ADLS/staging options | usually drop; note if used |
| `skipDuplicateMapInputs/Outputs` | mapping de-dup | usually drop |
| `broadcast` (join) | broadcast hint | `broadcast(df)` |
| `joinType`, `matchType` | join semantics | `.join(..., how=...)` |

## 8. Quoting and escapes

- String literals use single quotes: `'parquet'`, `'Texas'`. Inside expressions, single quotes delimit strings.
- Column/expression bodies can contain operators `== != && || ! + - * / < > <=  >=`, function calls, and dotted member access.
- Backtick-quote identifiers with special characters when emitting Spark (`` col("`odd name`") ``).
