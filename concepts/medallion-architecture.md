# Medallion Architecture

## What is Medallion Architecture?

Medallion Architecture is a **data design pattern** used to organize data in a lakehouse into **three progressive layers**: Bronze, Silver, and Gold. Each layer represents a different stage of data quality and refinement.

The name comes from the idea of medals — raw data starts as Bronze and is progressively refined until it reaches Gold quality.

---

## The Three Layers

```
Raw Source Data
      │
      ▼
┌─────────────┐
│   BRONZE    │  Raw, unprocessed data
│  (Landing)  │  "Store everything as-is"
└─────────────┘
      │
      ▼
┌─────────────┐
│   SILVER    │  Cleaned and validated data
│  (Refined)  │  "Make it trustworthy"
└─────────────┘
      │
      ▼
┌─────────────┐
│    GOLD     │  Aggregated, business-ready data
│  (Curated)  │  "Make it useful"
└─────────────┘
      │
      ▼
  Dashboards / Reports / ML Models
```

---

## Bronze Layer — Raw Data

The Bronze layer stores data **exactly as it arrives** from the source, with no transformations applied.

**Characteristics:**
- Data is stored in its original format (JSON, CSV, Parquet, etc.)
- No cleaning or validation
- Acts as a historical archive
- Append-only (data is never deleted)

**Example:**
```python
# Reading raw data from S3 and writing to Bronze
df_raw = spark.read.format("json").load("s3://bucket/raw/customers/")

df_raw.write \
    .format("delta") \
    .mode("append") \
    .save("/Volumes/workspace/default/spark_dev/bronze/customers")
```

---

## Silver Layer — Cleaned Data

The Silver layer contains data that has been **cleaned, validated, and standardized**.

**Characteristics:**
- Null values handled
- Data types corrected
- Duplicate records removed
- Column names standardized
- Basic business rules applied

**Example:**
```python
from pyspark.sql.functions import col, trim, upper

df_bronze = spark.read.format("delta").load("/path/bronze/customers")

df_silver = df_bronze \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("name", trim(upper(col("name")))) \
    .withColumn("age", col("age").cast("integer"))

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/path/silver/customers")
```

---

## Gold Layer — Business-Ready Data

The Gold layer contains **aggregated and enriched data**, ready for consumption by dashboards, reports, and machine learning models.

**Characteristics:**
- Data is joined across multiple Silver tables
- Aggregations and metrics are pre-computed
- Optimized for query performance
- Serves the final business use case

**Example:**
```python
df_customers = spark.read.format("delta").load("/path/silver/customers")
df_orders = spark.read.format("delta").load("/path/silver/orders")

df_gold = df_customers.join(df_orders, "customer_id") \
    .groupBy("customer_id", "name") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("order_value").alias("total_revenue")
    )

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/path/gold/customer_summary")
```

---

## Layer Comparison

| | Bronze | Silver | Gold |
|---|---|---|---|
| Data Quality | Raw | Clean | Curated |
| Transformations | None | Cleaning & Validation | Aggregations & Joins |
| Consumers | Engineers | Engineers / Analysts | Analysts / BI / ML |
| Write Mode | Append | Overwrite / Merge | Overwrite |
| Format | Any | Delta | Delta |

---

## Why Use Medallion Architecture?

- **Traceability** — raw data is always preserved in Bronze
- **Separation of concerns** — each layer has a clear responsibility
- **Reprocessing** — if something breaks in Silver, you can reprocess from Bronze
- **Performance** — Gold layer is optimized for fast queries
- **Collaboration** — different teams consume different layers based on their needs

---

## How This Project Uses It

This project implements a full Bronze → Silver → Gold pipeline using PySpark and Delta Lake on Databricks:

| Layer | Path | Description |
|---|---|---|
| Bronze | `/Volumes/workspace/default/spark_dev/bronze/` | Raw ingested data |
| Silver | `/Volumes/workspace/default/spark_dev/silver/` | Cleaned and validated data |
| Gold | `/Volumes/workspace/default/spark_dev/gold/` | Aggregated business metrics |
