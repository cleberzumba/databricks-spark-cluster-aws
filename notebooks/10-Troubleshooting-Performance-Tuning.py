# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 11: Troubleshooting & Performance Tuning
# MAGIC
# MAGIC - Troubleshooting and Tuning Apache Spark DataFrame API Applications 
# MAGIC   - Implement performance tuning strategies & optimize cluster utilization
# MAGIC   - **Partitioning, repartitioning, coalescing** 
# MAGIC   - **Identifying data skew** and reducing shuffling
# MAGIC   - **Describe Adaptive Query Execution (AQE)** and its benefits
# MAGIC   - **Perform logging and monitoring** - Driver logs and Executor logs
# MAGIC   - **Diagnose out-of-memory errors, cluster underutilization**
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Adaptive Query Execution (AQE)** - What it does automatically
# MAGIC - **Partitioning strategies** - Partitioning vs Bucketing
# MAGIC - **Data skew detection** - Symptoms and solutions
# MAGIC - **Logging** - How to analyze Driver and Executor logs
# MAGIC - **Common performance issues** - OOM, slow jobs, underutilization

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 1: Adaptive Query Execution (AQE)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is Adaptive Query Execution?
# MAGIC
# MAGIC ### EXAMPLE
# MAGIC "Describe Adaptive Query Execution (AQE) and its benefits"
# MAGIC
# MAGIC ### Definition:
# MAGIC **AQE dynamically optimizes query execution plans AT RUNTIME based on actual data statistics**
# MAGIC
# MAGIC ### What AQE Does Automatically:
# MAGIC
# MAGIC 1. **Coalesces Shuffle Partitions**
# MAGIC    - Reduces 200 partitions → fewer partitions if data is small
# MAGIC    - Avoids too many small tasks
# MAGIC
# MAGIC 2. **Handles Data Skew**
# MAGIC    - Splits large partitions into smaller ones
# MAGIC    - Balances work across executors
# MAGIC
# MAGIC 3. **Switches Join Strategies**
# MAGIC    - Changes Sort Merge Join → Broadcast Hash Join if table is small
# MAGIC    - Optimizes based on actual data size
# MAGIC
# MAGIC ### How to Enable:
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")  # Default in Spark 3.2+
# MAGIC ```
# MAGIC
# MAGIC ### BENEFITS:
# MAGIC - **Automatic optimization** - No manual tuning needed
# MAGIC - **Handles skew** - Splits skewed partitions
# MAGIC - **Reduces shuffle** - Coalesces small partitions
# MAGIC - **Switches join strategies** - Picks best join type
# MAGIC - **Runtime decisions** - Based on actual data, not estimates

# COMMAND ----------

# Check if AQE is enabled
print(f"AQE Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# Enable AQE (if not already)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Enable specific AQE features
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

print(" AQE is now configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. AQE in Action - Example

# COMMAND ----------

from pyspark.sql.functions import col, lit, rand

# Create sample data with skew
data_normal = [(i, f"user_{i % 100}") for i in range(10000)]
data_skewed = [(10000, "popular_user") for _ in range(50000)]  # 50k rows for one user!

all_data = data_normal + data_skewed
df = spark.createDataFrame(all_data, ["id", "user"])

# Group by user (will have skew - one user has 50k rows!)
result = df.groupBy("user").count()

print("Without AQE, this would have severe skew!")
print("With AQE, Spark automatically splits the skewed partition")

result.show(10)

# Check execution plan
result.explain()
# Look for "AdaptiveSparkPlan" in the output

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 2: Partitioning Strategies
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Partitioning vs Bucketing
# MAGIC
# MAGIC | Feature      | Partitioning                            | Bucketing                              |
# MAGIC |--------------|-----------------------------------------|----------------------------------------|
# MAGIC | **Storage**  | Directory-based (physical folders)      | Hash-based files                       |
# MAGIC | **Best for** | Low cardinality (year, month, country)  | High cardinality (user_id, product_id) |
# MAGIC | **Benefit**  | Partition pruning (skip entire folders) | Avoids shuffle on joins/aggregations   |
# MAGIC | **Example**  | `/year=2024/month=01/`                  | Files organized by hash of key         |
# MAGIC | **Syntax**   | `.partitionBy("col")`                   | `.bucketBy(n, "col")`                  |
# MAGIC
# MAGIC ### When to Use Partitioning:
# MAGIC - Filter queries on partition column (`WHERE year = 2024`)
# MAGIC - Low cardinality columns (< 1000 unique values)
# MAGIC - Time-based data (dates, years, months)
# MAGIC
# MAGIC ### When to Use Bucketing:
# MAGIC - Joins on high cardinality columns
# MAGIC - Repeated aggregations on same key
# MAGIC - Avoid shuffle for operations on bucketed column

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partitioning Example

# COMMAND ----------

# Create sample sales data
sales_data = [
    ("2024-01-15", "Electronics", 1200, "USA"),
    ("2024-01-16", "Clothing", 500, "UK"),
    ("2024-02-10", "Electronics", 800, "USA"),
    ("2024-02-11", "Clothing", 300, "UK"),
]

df_sales = spark.createDataFrame(sales_data, ["date", "category", "amount", "country"])

# Write with partitioning
output_path = "/tmp/sales_partitioned"
(df_sales.write
    .mode("overwrite")
    .partitionBy("country", "category")  # Creates folder structure
    .parquet(output_path))

print(f"✅ Data written to {output_path}")
print("Folder structure: /country=USA/category=Electronics/")

# Read with partition pruning (only reads relevant partitions)
df_filtered = (spark.read.parquet(output_path)
    .filter(col("country") == "USA"))  # Only reads USA partition!

print("\n🚀 Partition pruning in action - only USA data read")
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bucketing Example

# COMMAND ----------

# Bucketing example (for tables only, not files)
# Note: Bucketing requires saveAsTable

# Create bucketed table
df_sales.write.mode("overwrite") \
    .bucketBy(4, "country") \
    .sortBy("amount") \
    .saveAsTable("sales_bucketed")

print(" Created bucketed table 'sales_bucketed'")
print("Benefit: Joins on 'country' will avoid shuffle!")

# Join on bucketed column (no shuffle!)
df2 = spark.table("sales_bucketed")
result = df2.join(df2, "country")  # No shuffle because both are bucketed on 'country'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 3: Repartition vs Coalesce
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. repartition() vs coalesce()
# MAGIC
# MAGIC ### CRITICAL DIFFERENCES:
# MAGIC
# MAGIC | Feature           | repartition()          | coalesce()              |
# MAGIC |-------------------|------------------------|-------------------------|
# MAGIC | **Shuffle?**      | Yes (full shuffle)     | No shuffle              |
# MAGIC | **Can increase?** | Yes                    | No (silent failure!)    |
# MAGIC | **Can decrease?** | Yes                    | Yes                     |
# MAGIC | **Distribution**  | Even                   | May be uneven           |
# MAGIC | **Use when**      | Need even distribution | Reduce before write     |
# MAGIC
# MAGIC ### EXAMPLE:
# MAGIC ```python
# MAGIC df_100 = df.repartition(100)
# MAGIC result = df_100.coalesce(200)  # Still 100! Can't increase!
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Each:
# MAGIC
# MAGIC **Use repartition():**
# MAGIC - Need to **increase** partitions
# MAGIC - Need **even distribution** (hash partitioning)
# MAGIC - Repartition by specific column: `df.repartition("key_column")`
# MAGIC
# MAGIC **Use coalesce():**
# MAGIC - **Reduce** partitions before writing
# MAGIC - Want to **avoid shuffle** overhead
# MAGIC - Combine small partitions into larger ones

# COMMAND ----------

# Create DataFrame with many partitions
df_many = spark.range(1000000).repartition(100)
print(f"Initial partitions: {df_many.rdd.getNumPartitions()}")

# Reduce with coalesce (no shuffle)
df_coalesced = df_many.coalesce(10)
print(f"After coalesce(10): {df_coalesced.rdd.getNumPartitions()}")

# Try to increase with coalesce (doesn't work!)
df_coalesce_fail = df_coalesced.coalesce(50)
print(f"After coalesce(50): {df_coalesce_fail.rdd.getNumPartitions()}")  # Still 10!

# Increase with repartition (works, but shuffles)
df_repartitioned = df_coalesced.repartition(50)
print(f"After repartition(50): {df_repartitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Repartitioning by Column (Hash Partitioning)

# COMMAND ----------

# Create sample user activity data
activity = spark.range(10000).select(
    col("id"),
    (col("id") % 100).alias("user_id"),
    (rand() * 1000).alias("amount")
)

# Repartition by user_id (all records for same user go to same partition)
repartitioned_by_user = activity.repartition("user_id")

print(f"Partitions: {repartitioned_by_user.rdd.getNumPartitions()}")
print("All records for same user_id are now in same partition!")

# MAGIC %md
# MAGIC **Benefit**: Operations on user_id (joins, groupBy) won't need shuffle!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 4: Data Skew Detection & Solutions
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. What is Data Skew?
# MAGIC
# MAGIC ### Definition:
# MAGIC **Uneven distribution of data across partitions**
# MAGIC
# MAGIC ### Example:
# MAGIC ```
# MAGIC Partition 1: 100 rows    ← Small
# MAGIC Partition 2: 150 rows    ← Small
# MAGIC Partition 3: 1,000,000 rows  ← HUGE! (Skewed)
# MAGIC Partition 4: 120 rows    ← Small
# MAGIC ```
# MAGIC
# MAGIC ### Symptoms of Data Skew:
# MAGIC - One or few tasks take MUCH longer than others
# MAGIC - Most tasks finish quickly, job waits for stragglers
# MAGIC - Executor shows high CPU/memory for one task
# MAGIC - "Stage X: 99/100 tasks complete" stuck for long time
# MAGIC
# MAGIC ### Common Causes:
# MAGIC - NULL values in join/group key
# MAGIC - Popular values (e.g., "unknown", "N/A", one celebrity user)
# MAGIC - Uneven data distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Detecting Data Skew

# COMMAND ----------

# Create skewed data
normal_data = [(i % 100, f"value_{i}") for i in range(10000)]
skewed_data = [(999, f"popular_{i}") for i in range(100000)]  # 100k rows with key=999!

all_data = normal_data + skewed_data
df_skewed = spark.createDataFrame(all_data, ["key", "value"])

# Check distribution
print("Distribution of data by key:")
df_skewed.groupBy("key").count().orderBy(col("count").desc()).show(10)

# Key 999 has 100k rows while others have ~100 rows - SEVERE SKEW!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Solutions for Data Skew
# MAGIC
# MAGIC ### Solution 1: Filter Out Skewed Values (if appropriate)
# MAGIC ```python
# MAGIC df.filter(col("key").isNotNull())  # Remove NULLs
# MAGIC df.filter(col("key") != "popular_value")  # Filter known skewed values
# MAGIC ```
# MAGIC
# MAGIC ### Solution 2: Salt the Key (Advanced)
# MAGIC Add random value to distribute skewed key:
# MAGIC ```python
# MAGIC df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
# MAGIC ```
# MAGIC
# MAGIC ### Solution 3: Broadcast Join (if one side is small)
# MAGIC ```python
# MAGIC large_df.join(broadcast(small_df), "key")
# MAGIC ```
# MAGIC
# MAGIC ### Solution 4: Enable AQE (automatic!)
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC ```

# COMMAND ----------

# Solution 1: Broadcast join to avoid skew
df_small = spark.createDataFrame([(i, f"name_{i}") for i in range(100)], ["key", "name"])

# Without broadcast - may have skew issues
# result = df_skewed.join(df_small, "key")

# With broadcast - avoids shuffle, no skew!
from pyspark.sql.functions import broadcast
result = df_skewed.join(broadcast(df_small), "key")

print(" Using broadcast join to avoid skew")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 5: Logging & Monitoring
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Driver Logs vs Executor Logs
# MAGIC
# MAGIC "How should the engineer retrieve the Executor logs to diagnose performance issues?"
# MAGIC
# MAGIC **Answer: Use the Spark UI to select the stage and view the executor logs
# MAGIC directly from the stages tab.**
# MAGIC
# MAGIC ### Driver Logs:
# MAGIC - Shows job scheduling
# MAGIC - DAG creation
# MAGIC - Query plans
# MAGIC - Overall job progress
# MAGIC - **Location**: Where driver runs (client machine or cluster)
# MAGIC
# MAGIC ### Executor Logs:
# MAGIC - Shows task execution details
# MAGIC - Data processing errors
# MAGIC - Memory issues (OOM)
# MAGIC - Task failures
# MAGIC - **Location**: On worker nodes where executors run
# MAGIC
# MAGIC ### How to Access Logs:
# MAGIC
# MAGIC 1. **Spark UI** (EXAM ANSWER!):
# MAGIC    - Go to Spark UI
# MAGIC    - Click on "Stages" tab
# MAGIC    - Select the stage
# MAGIC    - View executor logs
# MAGIC
# MAGIC 2. **Command Line**:
# MAGIC    ```bash
# MAGIC    # Driver logs
# MAGIC    yarn logs -applicationId <app_id>
# MAGIC
# MAGIC    # Executor logs (on worker nodes)
# MAGIC    ls /var/log/spark/executor/
# MAGIC    ```
# MAGIC
# MAGIC 3. **Databricks**:
# MAGIC    - Cluster page → Driver Logs / Executor Logs tabs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Common Issues in Logs
# MAGIC
# MAGIC ### Out of Memory (OOM) Errors:
# MAGIC
# MAGIC **Symptoms in logs:**
# MAGIC ```
# MAGIC java.lang.OutOfMemoryError: Java heap space
# MAGIC Container killed by YARN for exceeding memory limits
# MAGIC ```
# MAGIC
# MAGIC **Causes:**
# MAGIC - Too much data per partition
# MAGIC - Collecting large datasets to driver
# MAGIC - Caching too much data
# MAGIC - Data skew
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Increase executor memory: `spark.executor.memory`
# MAGIC - Increase partition count: `spark.sql.shuffle.partitions`
# MAGIC - Use `.limit()` before `.collect()`
# MAGIC - Fix data skew
# MAGIC - Don't cache everything
# MAGIC
# MAGIC ### Cluster Underutilization:
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Few executors active
# MAGIC - Low CPU usage
# MAGIC - Jobs slow despite resources available
# MAGIC
# MAGIC **Causes:**
# MAGIC - Too few partitions (tasks < executor cores)
# MAGIC - Data skew (some executors idle while one works)
# MAGIC - Small dataset (doesn't need all resources)
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Increase partitions: `repartition()`
# MAGIC - Fix data skew
# MAGIC - Use dynamic allocation: `spark.dynamicAllocation.enabled`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Analyzing Query Performance

# COMMAND ----------

# Create sample query
sample_df = spark.range(1000000)

# Add timing to measure performance
import time

start = time.time()
result = (sample_df
    .filter(col("id") % 2 == 0)
    .groupBy((col("id") / 1000).cast("int").alias("bucket"))
    .count())

# Trigger execution
count = result.count()
end = time.time()

print(f"Query took: {end - start:.2f} seconds")
print(f"Result count: {count}")

# Analyze execution plan
print("\n=== EXECUTION PLAN ===")
result.explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Reading the Explain Plan

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Things to Look For:
# MAGIC
# MAGIC 1. **Exchange** = Shuffle (expensive!)
# MAGIC    ```
# MAGIC    Exchange hashpartitioning(key, 200)
# MAGIC    ```
# MAGIC
# MAGIC 2. **BroadcastExchange** = Broadcast join (good!)
# MAGIC    ```
# MAGIC    BroadcastExchange
# MAGIC    ```
# MAGIC
# MAGIC 3. **Filter pushdown** (good!)
# MAGIC    ```
# MAGIC    PushedFilters: [IsNotNull(col), GreaterThan(col,100)]
# MAGIC    ```
# MAGIC
# MAGIC 4. **Number of partitions**
# MAGIC    ```
# MAGIC    hashpartitioning(key, 200)  ← 200 partitions
# MAGIC    ```

# COMMAND ----------

# Example with explain modes
sample_query = df_skewed.groupBy("key").count()

# Physical plan only (default)
print("=== PHYSICAL PLAN ===")
sample_query.explain()

# All plans (parsed, analyzed, optimized, physical)
print("\n=== EXTENDED PLAN ===")
sample_query.explain(extended=True)

# Formatted output
print("\n=== FORMATTED PLAN ===")
sample_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 6: Performance Tuning Best Practices
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Performance Tuning Checklist
# MAGIC
# MAGIC ### 1. Partitioning:
# MAGIC - Use appropriate number of partitions
# MAGIC   - Too few: Underutilization
# MAGIC   - Too many: Overhead
# MAGIC - Rule of thumb: 2-4 partitions per CPU core
# MAGIC - Adjust `spark.sql.shuffle.partitions` for your data size
# MAGIC
# MAGIC ### 2. Caching:
# MAGIC - Cache DataFrames used multiple times
# MAGIC - Use appropriate storage level
# MAGIC - Don't cache everything (memory pressure!)
# MAGIC
# MAGIC ### 3. Broadcasting:
# MAGIC - Broadcast small tables (< 10MB)
# MAGIC - Use `broadcast()` hint
# MAGIC - Don't broadcast large tables
# MAGIC
# MAGIC ### 4. Data Skew:
# MAGIC - Enable AQE
# MAGIC - Filter out skewed values if appropriate
# MAGIC - Use salting for extreme skew
# MAGIC
# MAGIC ### 5. File Format:
# MAGIC - Use Parquet for analytics
# MAGIC - Use partitioning for time-series data
# MAGIC - Use bucketing for repeated joins
# MAGIC
# MAGIC ### 6. Avoid Common Mistakes:
# MAGIC - Don't use `.collect()` on large DataFrames
# MAGIC - Don't use UDFs when built-in functions exist
# MAGIC - Don't create too many small files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Configuration Parameters for Performance
# MAGIC
# MAGIC ### Key Spark Configurations:

# COMMAND ----------

# Set performance-related configurations

# Shuffle partitions (default: 200)
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Broadcast join threshold (default: 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Memory settings (set when creating session)
# spark.conf.set("spark.executor.memory", "4g")
# spark.conf.set("spark.driver.memory", "2g")

print(" Performance configurations set")

# View current settings
print(f"\nShuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"Broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary & Exam Tips
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Adaptive Query Execution (AQE):
# MAGIC **What AQE does automatically:**
# MAGIC - Coalesces shuffle partitions
# MAGIC - Handles data skew (splits large partitions)
# MAGIC - Switches join strategies (SMJ → Broadcast)
# MAGIC
# MAGIC **Enable:** `spark.sql.adaptive.enabled = true` (default Spark 3.2+)
# MAGIC
# MAGIC ### 2. Partitioning vs Bucketing:
# MAGIC | Feature  | Partitioning      | Bucketing        |
# MAGIC |----------|-------------------|------------------|
# MAGIC | Storage  | Directory-based   | Hash-based files |
# MAGIC | Best for | Low cardinality   | High cardinality |
# MAGIC | Benefit  | Partition pruning | Avoids shuffle   |
# MAGIC
# MAGIC ### 3. repartition() vs coalesce():
# MAGIC | Feature       | repartition()     | coalesce()          |
# MAGIC |---------------|-------------------|---------------------|
# MAGIC | Shuffle?      | Yes               | No                  |
# MAGIC | Can increase? | Yes               | No!                 |
# MAGIC | Use when      | Even distribution | Reduce before write |
# MAGIC
# MAGIC ### 4. Data Skew Solutions:
# MAGIC 1. Enable AQE
# MAGIC 2. Broadcast join (if one side small)
# MAGIC 3. Filter skewed values
# MAGIC 4. Salt the key (advanced)
# MAGIC
# MAGIC ### 5. Logging:
# MAGIC **How to view Executor logs?**
# MAGIC - Use Spark UI → Stages tab → View executor logs
# MAGIC - Driver logs: Overall job progress
# MAGIC - Executor logs: Task execution details, errors
# MAGIC
# MAGIC ### 6. Common Performance Issues:
# MAGIC
# MAGIC **OOM Errors:**
# MAGIC - Increase executor memory
# MAGIC - Increase partition count
# MAGIC - Fix data skew
# MAGIC
# MAGIC **Cluster Underutilization:**
# MAGIC - Increase partitions
# MAGIC - Fix data skew
# MAGIC - Use dynamic allocation
# MAGIC
