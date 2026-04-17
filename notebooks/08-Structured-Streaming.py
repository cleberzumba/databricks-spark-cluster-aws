# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 8: Structured Streaming Basics
# MAGIC
# MAGIC - Section 5: Structured Streaming
# MAGIC   - Explain the Structured Streaming engine
# MAGIC   - Programming model, micro-batch processing
# MAGIC   - Exactly-once semantics, fault tolerance
# MAGIC   - Create and write Streaming DataFrames
# MAGIC   - Output modes and output sinks
# MAGIC   - Basic operations: selection, projection, window, aggregation
# MAGIC   - Streaming deduplication
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Streaming DataFrame** vs Batch DataFrame
# MAGIC - **Output Modes**: append, update, complete
# MAGIC - **Output Sinks**: console, file, memory, foreach
# MAGIC - **Triggers**: micro-batch intervals
# MAGIC - **Watermarks**: handle late data
# MAGIC - **Window operations**: tumbling, sliding
# MAGIC - **Deduplication**: with and without watermarks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Why Use Structured Streaming? 
# MAGIC
# MAGIC **Scenario:**
# MAGIC "Calculate real-time, rolling metrics like 'average session duration in last hour'
# MAGIC that update every 2 minutes. Why use Streaming DataFrames?"
# MAGIC
# MAGIC **Answer: Streaming DataFrames enable continuous data processing with
# MAGIC incremental updates to aggregations, allowing real-time metrics WITHOUT
# MAGIC reprocessing the entire dataset every 2 minutes.**
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC - **Incremental processing** - Only new data processed
# MAGIC - **Real-time updates** - Results update as data arrives
# MAGIC - **No full reprocessing** - Much more efficient than batch
# MAGIC - **Same API as batch** - Easy to learn
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Real-time dashboards
# MAGIC - Continuous aggregations
# MAGIC - Streaming ETL
# MAGIC - Real-time alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating a Streaming DataFrame
# MAGIC
# MAGIC ### readStream vs read:
# MAGIC - `spark.read` - Batch (one-time read)
# MAGIC - `spark.readStream` - Streaming (continuous read)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, sum, current_timestamp

# Create sample streaming source (from rate source)
# Rate source generates rows with timestamp and value
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)  # 5 rows per second
    .load())

# Check if it's streaming
print(f"Is streaming: {streaming_df.isStreaming}")

streaming_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Streaming Operations
# MAGIC
# MAGIC ### Same Operations as Batch:
# MAGIC - select(), filter(), withColumn()
# MAGIC - Transformations are lazy until writeStream

# COMMAND ----------

# Apply transformations (lazy evaluation)
processed_stream = (streaming_df
    .withColumn("value_squared", col("value") * col("value"))
    .filter(col("value") % 2 == 0)  # Only even values
)

# Nothing happens yet - need to start the query!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Output Modes 
# MAGIC
# MAGIC ### Three Output Modes:
# MAGIC
# MAGIC | Mode         | Behavior                | Use Case                    |
# MAGIC |--------------|-------------------------|-----------------------------|
# MAGIC | **append**   | Only new rows (default) | Non-aggregated queries      |
# MAGIC | **update**   | New + updated rows      | Aggregations with updates   |
# MAGIC | **complete** | Entire result table     | Aggregations, small results |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Mode: APPEND

# COMMAND ----------

# Append mode - only new rows
# Works for non-aggregated queries
query_append = (streaming_df
    .select("timestamp", "value")
    .writeStream
    .format("console")
    .outputMode("append")
    .start())

# Let it run for a few seconds
import time
time.sleep(10)

query_append.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Mode: UPDATE

# COMMAND ----------

# Update mode - new AND updated rows
# Used for aggregations
query_update = (streaming_df
    .groupBy("value")
    .count()
    .writeStream
    .format("console")
    .outputMode("update")
    .start())

time.sleep(10)
query_update.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Mode: COMPLETE

# COMMAND ----------

# Complete mode - entire result table
# Shows all groups, even if not updated
query_complete = (streaming_df
    .groupBy("value")
    .count()
    .writeStream
    .format("console")
    .outputMode("complete")
    .start())

time.sleep(10)
query_complete.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Output Sinks
# MAGIC
# MAGIC ### Common Sinks:
# MAGIC - **console** - Display to console (debugging)
# MAGIC - **memory** - In-memory table (debugging)
# MAGIC - **file** - Parquet, JSON, CSV, ORC
# MAGIC - **kafka** - Apache Kafka
# MAGIC - **foreach** / **foreachBatch** - Custom processing

# COMMAND ----------

# Console sink (for debugging)
query1 = (streaming_df
    .writeStream
    .format("console")
    .outputMode("append")
    .start())

# Memory sink (creates temp table)
query2 = (streaming_df
    .writeStream
    .format("memory")
    .queryName("my_stream_table")
    .outputMode("append")
    .start())

# Query the memory table
time.sleep(5)
spark.sql("SELECT * FROM my_stream_table LIMIT 10").show()

query1.stop()
query2.stop()

# COMMAND ----------

# File sink (Parquet)
checkpoint_path = "/tmp/streaming_checkpoint"
output_path = "/tmp/streaming_output"

query_file = (streaming_df
    .writeStream
    .format("parquet")
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .outputMode("append")
    .start())

time.sleep(10)
query_file.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Triggers - Control Processing Frequency

# COMMAND ----------

from pyspark.sql.streaming import Trigger

# Micro-batch every 10 seconds
query_trigger = (streaming_df
    .writeStream
    .format("console")
    .trigger(processingTime="10 seconds")
    .start())

time.sleep(15)
query_trigger.stop()

# Trigger once (process available data and stop)
query_once = (streaming_df
    .writeStream
    .format("console")
    .trigger(once=True)
    .start())

query_once.awaitTermination()

# Continuous processing (low latency, experimental)
# query_continuous = (streaming_df
#     .writeStream
#     .format("console")
#     .trigger(continuous="1 second")
#     .start())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Aggregations in Streaming
# MAGIC
# MAGIC ### SCENARIO:
# MAGIC "Calculate average session duration in last hour, updating every 2 minutes"

# COMMAND ----------

# Simple aggregation (running total)
query_agg = (streaming_df
    .groupBy("value")
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value")
    )
    .writeStream
    .format("console")
    .outputMode("complete")  # Show all groups
    .start())

time.sleep(15)
query_agg.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Window Operations - Time-based Aggregations
# MAGIC
# MAGIC ### Window Types:
# MAGIC - **Tumbling window**: Non-overlapping, fixed-size
# MAGIC - **Sliding window**: Overlapping windows

# COMMAND ----------

# Tumbling window (10-second windows)
windowed_stream = (streaming_df
    .groupBy(window(col("timestamp"), "10 seconds"))
    .agg(count("*").alias("count"))
    .writeStream
    .format("console")
    .outputMode("complete")
    .start())

time.sleep(30)
windowed_stream.stop()

# COMMAND ----------

# Sliding window (10-second windows, sliding every 5 seconds)
sliding_stream = (streaming_df
    .groupBy(window(col("timestamp"), "10 seconds", "5 seconds"))
    .agg(count("*").alias("count"))
    .writeStream
    .format("console")
    .outputMode("complete")
    .start())

time.sleep(30)
sliding_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Streaming Deduplication
# MAGIC
# MAGIC ### Two Approaches:
# MAGIC 1. **Without watermark** - Keep ALL seen keys (memory grows!)
# MAGIC 2. **With watermark** - Drop old keys (bounded memory)

# COMMAND ----------

# Deduplication without watermark
# Memory grows unbounded!
dedup_stream = (streaming_df
    .dropDuplicates(["value"])
    .writeStream
    .format("console")
    .outputMode("append")
    .start())

time.sleep(15)
dedup_stream.stop()

# COMMAND ----------

# Deduplication WITH watermark (recommended!)
# Only keeps state for recent data
from pyspark.sql.functions import expr

watermarked_stream = (streaming_df
    .withWatermark("timestamp", "1 minute")  # 1-minute watermark
    .dropDuplicates(["value", "timestamp"])
    .writeStream
    .format("console")
    .outputMode("append")
    .start())

time.sleep(15)
watermarked_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Watermarks - Handle Late Data
# MAGIC
# MAGIC ### What are Watermarks:
# MAGIC - Threshold for how late data can arrive
# MAGIC - Allows Spark to drop old state
# MAGIC - Prevents unbounded state growth

# COMMAND ----------

# Example: 10-minute watermark
# Drops state for data older than (max_timestamp - 10 minutes)
watermark_example = (streaming_df
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("value")
    )
    .count()
    .writeStream
    .format("console")
    .outputMode("update")
    .start())

time.sleep(20)
watermark_example.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Selections and Projections in Streaming

# COMMAND ----------

# Same as batch DataFrame operations
selected_stream = (streaming_df
    .select(
        col("timestamp"),
        col("value"),
        (col("value") * 2).alias("doubled"),
        (col("value") % 2 == 0).alias("is_even")
    )
    .filter(col("value") > 10)
    .writeStream
    .format("console")
    .outputMode("append")
    .start())

time.sleep(10)
selected_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Exactly-Once Semantics
# MAGIC
# MAGIC - Structured Streaming provides **exactly-once** guarantees
# MAGIC - Uses checkpointing and write-ahead logs
# MAGIC - Idempotent sinks ensure no duplicates

# COMMAND ----------

# Checkpointing ensures exactly-once
checkpoint_dir = "/tmp/checkpoint_demo"

exactly_once_query = (streaming_df
    .groupBy("value")
    .count()
    .writeStream
    .format("parquet")
    .option("checkpointLocation", checkpoint_dir)
    .option("path", "/tmp/output_demo")
    .outputMode("complete")
    .start())

time.sleep(10)
exactly_once_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Monitoring Streaming Queries

# COMMAND ----------

# Get active streams
active_streams = spark.streams.active
print(f"Active streams: {len(active_streams)}")

# Start a query
monitor_query = (streaming_df
    .writeStream
    .format("console")
    .start())

# Get status
print(f"Query ID: {monitor_query.id}")
print(f"Run ID: {monitor_query.runId}")
print(f"Is active: {monitor_query.isActive}")

# Get recent progress
time.sleep(5)
print(monitor_query.lastProgress)

monitor_query.stop()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# SCENARIO:
# "Calculate rolling metrics from continuous clickstream data,
# updating every 2 minutes"

# CORRECT APPROACH: Streaming DataFrame
rolling_metrics = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("session_duration", col("value") % 100)  # Simulated duration
    .withWatermark("timestamp", "1 hour")  # Handle late data
    .groupBy(window(col("timestamp"), "1 hour", "2 minutes"))  # 1-hour window, slide every 2 min
    .agg(
        avg("session_duration").alias("avg_session_duration"),
        count("*").alias("event_count")
    )
    .writeStream
    .format("console")
    .outputMode("update")  # Show updated windows
    .trigger(processingTime="2 minutes")  # Update every 2 minutes
    .start())

time.sleep(15)
rolling_metrics.stop()

# WHY Streaming DataFrames?
# Incremental updates - doesn't reprocess all data
# Real-time metrics
# Efficient - only processes new data

# COMMAND ----------
