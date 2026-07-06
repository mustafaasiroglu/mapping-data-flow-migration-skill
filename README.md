# Mapping Data Flow → Fabric Spark Skill

A GitHub Copilot [skill](.github/skills/mdf-to-fabric-spark/SKILL.md) that converts an Azure
Data Factory / Synapse **mapping data flow** (the Data Flow Script behind it) into an
equivalent, runnable **Microsoft Fabric PySpark notebook** — plus a conversion report that
flags anything needing human review.

It uses semantic (LLM-driven) translation, so it handles the full breadth of MDF
transformations and expression functions, producing idiomatic PySpark (DataFrame API) where
each MDF stream becomes a DataFrame variable of the same name.

## Purpose

- Migrate ADF/Synapse mapping data flows to Fabric notebooks or Spark Job Definitions.
- Understand or document what an existing data flow does, transformation by transformation.

Not for ADF **pipeline** activities (Copy, ForEach, Lookup activity), triggers, linked
services, or wrangling data flows (Power Query) — only the mapping **data flow** graph.

## Usage in VS Code

1. Open this folder in VS Code with GitHub Copilot (agent mode / Copilot Chat).
2. Put your Data Flow Script in a file (grab it from the ADF/Synapse **Script** button, an
   ARM/JSON `dataflows` resource, or the Data Flow GET REST API). See [sample-mdf.txt](sample-mdf.txt).
3. Ask Copilot to run the skill, for example:

   > Follow the mdf-to-fabric-spark skill and convert `sample-mdf.txt`.

The skill discovers the graph, maps each transformation and expression to PySpark, and writes:

- `<DataFlowName>.ipynb` — Fabric-ready PySpark notebook (one cell per transformation).
- `<DataFlowName>_conversion_report.md` — parameter/transformation mapping and review items.

See the worked example: [WideWorldImportersDW_City_Sales.ipynb](WideWorldImportersDW_City_Sales.ipynb)
and its [conversion report](WideWorldImportersDW_City_Sales_conversion_report.md).

## Repository layout

| Path | Purpose |
|------|---------|
| [.github/skills/mdf-to-fabric-spark/SKILL.md](.github/skills/mdf-to-fabric-spark/SKILL.md) | Skill entry point and procedure |
| `.github/skills/mdf-to-fabric-spark/references/` | Grammar, transformation & expression mappings, Fabric guide, validation |
| `.github/skills/mdf-to-fabric-spark/scripts/parse_mdf.py` | Tokenizes a DFS into ordered transformation blocks |
| `.github/skills/mdf-to-fabric-spark/assets/` | Notebook and conversion-report templates |
| `.github/skills/mdf-to-fabric-spark/examples/` | Full worked conversion |
| [sample-mdf.txt](sample-mdf.txt) | Example Data Flow Script input |
