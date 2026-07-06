# Worked Example — WideWorldImporters City/Sales

A complete conversion of [../../../../sample-mdf.txt](../../../../sample-mdf.txt) showing the
target quality: MDF Data Flow Script → Fabric PySpark notebook. Use this as the reference
pattern for structure, naming, and comment style.

## Input (DFS, abbreviated)

```
parameters{ Container as string ("data"), CityFolderPath ..., SalesFolderPath ...,
  SinkContainer ..., SinkFolderPath ..., StateFilter1 as string ("Texas"),
  StateFilter2 as string ("California") }

source(output(CityKey as integer, ..., LatestRecordedPopulation as long,
  ValidFrom as timestamp, ...), format:'parquet', fileSystem:($Container),
  folderPath:($CityFolderPath)) ~> dimcity
source(output(SaleKey as long, CityKey as integer, ..., UnitPrice as decimal(18,2),
  TaxRate as decimal(18,3), ...), format:'parquet', fileSystem:($Container),
  folderPath:($SalesFolderPath)) ~> sales
sales filter(TotalExcludingTax>700 && Profit>500) ~> salesfilter
dimcity filter(StateProvince==$StateFilter1 || StateProvince==$StateFilter2) ~> cityfilter
cityfilter select(mapColumn(CityKey, City, State = StateProvince, Country, Continent,
  SalesTerritory, Region, Subregion), ...) ~> cityselect
salesfilter select(mapColumn(CityKey, InvoiceDate = InvoiceDateKey,
  DeliveryDate = DeliveryDateKey, Salesperson = SalespersonKey, Package, Quantity,
  UnitPrice, TaxRate, TotalWithoutTax = TotalExcludingTax, TaxAmount, Profit,
  TotalWithTax = TotalIncludingTax), ...) ~> salesselect
salesselect, cityselect join((salesselect@CityKey) == (cityselect@CityKey),
  joinType:'left', matchType:'exact', broadcast:'auto') ~> joinwithcity
joinwithcity select(mapColumn(InvoiceDate, DeliveryDate, Quantity, UnitPrice, TaxRate,
  TotalWithoutTax, TaxAmount, Profit, TotalWithTax, City, State, Country, Continent,
  SalesTerritory, Region, Subregion), ...) ~> joinselect
joinselect sink(format:'parquet', fileSystem:($SinkContainer),
  folderPath:($SinkFolderPath)) ~> sinktostorage
```

DAG: `dimcity → cityfilter → cityselect ↘  join → joinselect → sink`
`sales → salesfilter → salesselect ↗`

## Output notebook (cell by cell)

### Cell 1 — markdown title
```markdown
# WideWorldImporters City/Sales
Converted from ADF mapping data flow. One cell per MDF transformation; variable = stream name.
```

### Cell 2 — parameters (tagged `parameters`)
```python
Container       = "data"
CityFolderPath  = "WideWorldImportersDW/parquet/full/dimension_city"
SalesFolderPath = "WideWorldImportersDW/parquet/full/fact_sale"
SinkContainer   = "data"
SinkFolderPath  = "SparkTestResult"
StateFilter1    = "Texas"
StateFilter2    = "California"

STORAGE_ACCOUNT = "TODO_set_me"   # ADLS Gen2 account; or set USE_ONELAKE=True
USE_ONELAKE     = False
```

### Cell 3 — imports & helper
```python
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

def storage_base(container):
    if USE_ONELAKE:
        return "/lakehouse/default/Files"
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
```

### Cell 4 — source `dimcity`
```python
# MDF: source(output(...)) ~> dimcity
dimcity = spark.read.format("parquet").load(f"{storage_base(Container)}/{CityFolderPath}")
```

### Cell 5 — source `sales`
```python
# MDF: source(output(...)) ~> sales
sales = spark.read.format("parquet").load(f"{storage_base(Container)}/{SalesFolderPath}")
```

### Cell 6 — `salesfilter`
```python
# MDF: sales filter(TotalExcludingTax>700 && Profit>500) ~> salesfilter
salesfilter = sales.filter((F.col("TotalExcludingTax") > 700) & (F.col("Profit") > 500))
```

### Cell 7 — `cityfilter`
```python
# MDF: dimcity filter(StateProvince==$StateFilter1 || StateProvince==$StateFilter2) ~> cityfilter
cityfilter = dimcity.filter(
    (F.col("StateProvince") == StateFilter1) | (F.col("StateProvince") == StateFilter2)
)
```

### Cell 8 — `cityselect`
```python
# MDF: cityfilter select(mapColumn(..., State = StateProvince, ...)) ~> cityselect
cityselect = cityfilter.select(
    F.col("CityKey"),
    F.col("City"),
    F.col("StateProvince").alias("State"),
    F.col("Country"),
    F.col("Continent"),
    F.col("SalesTerritory"),
    F.col("Region"),
    F.col("Subregion"),
)
```

### Cell 9 — `salesselect`
```python
# MDF: salesfilter select(mapColumn(CityKey, InvoiceDate = InvoiceDateKey, ...)) ~> salesselect
salesselect = salesfilter.select(
    F.col("CityKey"),
    F.col("InvoiceDateKey").alias("InvoiceDate"),
    F.col("DeliveryDateKey").alias("DeliveryDate"),
    F.col("SalespersonKey").alias("Salesperson"),
    F.col("Package"),
    F.col("Quantity"),
    F.col("UnitPrice"),
    F.col("TaxRate"),
    F.col("TotalExcludingTax").alias("TotalWithoutTax"),
    F.col("TaxAmount"),
    F.col("Profit"),
    F.col("TotalIncludingTax").alias("TotalWithTax"),
)
```

### Cell 10 — `joinwithcity`
```python
# MDF: salesselect, cityselect join((salesselect@CityKey)==(cityselect@CityKey),
#       joinType:'left', broadcast:'auto') ~> joinwithcity
# Note: both sides have CityKey. Use aliases to disambiguate; keep the left (sales) CityKey.
joinwithcity = salesselect.alias("s").join(
    cityselect.alias("c"),
    F.col("s.CityKey") == F.col("c.CityKey"),
    how="left",
)
```

### Cell 11 — `joinselect`
```python
# MDF: joinwithcity select(mapColumn(InvoiceDate, ..., City, State, ...)) ~> joinselect
# Explicit projection resolves the duplicate CityKey by simply not selecting it.
joinselect = joinwithcity.select(
    F.col("InvoiceDate"),
    F.col("DeliveryDate"),
    F.col("Quantity"),
    F.col("UnitPrice"),
    F.col("TaxRate"),
    F.col("TotalWithoutTax"),
    F.col("TaxAmount"),
    F.col("Profit"),
    F.col("TotalWithTax"),
    F.col("City"),
    F.col("State"),
    F.col("Country"),
    F.col("Continent"),
    F.col("SalesTerritory"),
    F.col("Region"),
    F.col("Subregion"),
)
```

### Cell 12 — sink `sinktostorage`
```python
# MDF: joinselect sink(format:'parquet', fileSystem:($SinkContainer),
#       folderPath:($SinkFolderPath)) ~> sinktostorage
(joinselect.write
    .format("parquet")
    .mode("overwrite")
    .save(f"{storage_base(SinkContainer)}/{SinkFolderPath}"))
```

### Cell 13 — validation (optional)
```python
print("dimcity:", dimcity.count(), "sales:", sales.count())
print("salesfilter:", salesfilter.count(), "cityfilter:", cityfilter.count())
print("joinselect:", joinselect.count())
joinselect.printSchema()
assert joinselect.filter(F.col("City").isNull()).count() >= 0  # left join may yield null City
```

## Key decisions illustrated

- **Stream = variable name** throughout, so the notebook maps 1:1 to the DFS.
- **`&&`/`||` → `&`/`|`** with every comparison parenthesized.
- **Rename mappings** (`State = StateProvince`) → `.alias()`.
- **Join duplicate column** (`CityKey` on both sides) resolved via `.alias("s")/.alias("c")` and dropped by the downstream explicit `select` (mirrors the MDF `joinselect`).
- **`broadcast:'auto'`** → left to Spark AQE (no explicit `F.broadcast`).
- **Review flags** for the report: `allowSchemaDrift:true` on both sources; `decimal(18,2)`/`decimal(18,3)` scale; left-join nullable City/State; unknown `STORAGE_ACCOUNT` path.
