# Databricks notebooks source

# MAGIC %md
# MAGIC Notebook 11: Shuffle - Occurs in wide operations (join, groupBy, distinct)
# MAGIC
# MAGIC - Broadcast vs Shuffle
# MAGIC - Execution Plan (`explain`)
# MAGIC - Join Strategies
# MAGIC - Partitioning
# MAGIC - Data Skew
# MAGIC - Catalyst Optimizer

# COMMAND ------------ 

# MAGIC %md
# MAGIC Example (generates shuffle)

# COMMAND ------------ 

df.groupBy("dept_id").count().explain("formatted")

# You will see:
# Exchange

# MAGIC %md
# MAGIC This means: **shuffle is happening**

# COMMAND ------------ 

# MAGIC %md
# MAGIC Execution Plan (`explain()`)
# MAGIC Example with join

from pyspark.sql.functions import broadcast

df = employees.join(broadcast(departments), "dept_id")
df.explain("formatted")

# MAGIC %md
# MAGIC What to look for:
# MAGIC
# MAGIC - `BroadcastHashJoin` -> broadcast applied  
# MAGIC - `Exchange` -> shuffle  
# MAGIC - `Scan` -> data reading  

# COMMAND ------------ 

# MAGIC %md
# MAGIC Join Strategies

# MAGIC %md
# MAGIC Broadcast Hash Join

employees.join(broadcast(departments), "dept_id").explain()

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC BroadcastHashJoin

# COMMAND ------------ 

# MAGIC %md
# MAGIC Sort Merge Join (pattern)

employees.join(departments, "dept_id").explain()

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC SortMergeJoin  
# MAGIC Exchange

# COMMAND ------------ 

# MAGIC %md
# MAGIC Partitioning
# MAGIC
# MAGIC Repartition (generates shuffle)

df = employees.repartition(10)
df.rdd.getNumPartitions()

# MAGIC %md
# MAGIC Coalesce (does NOT generate shuffle)

df = employees.coalesce(2)
df.rdd.getNumPartitions()

# COMMAND ------------ 

# MAGIC %md
# MAGIC Data Skew
# MAGIC
# MAGIC Simulating skew

data = [(1, "A")] * 1000000 + [(2, "B")] * 10
df = spark.createDataFrame(data, ["key", "value"])

df.groupBy("key").count().show()

# MAGIC %md
# MAGIC Key `1` concentrates most of the data

# MAGIC %md
# MAGIC Simple solution (salting)

from pyspark.sql.functions import rand

df_salted = df.withColumn("salt", (rand() * 10).cast("int"))

# COMMAND ------------ 

# MAGIC %md
# MAGIC Catalyst Optimizer
# MAGIC
# MAGIC Example (column pruning)

employees.select("emp_id").explain("formatted")

# MAGIC %md
# MAGIC Spark reads **only the required column**

# MAGIC %md
# MAGIC Predicate Pushdown

employees.filter("dept_id = 1").explain("formatted")

# MAGIC %md
# MAGIC Filter is applied during data reading (more efficient)

# COMMAND ------------ 

# MAGIC %md
# MAGIC Join Best Practices
# MAGIC
# MAGIC WRONG (expensive)

employees.join(departments, "dept_id")

# MAGIC %md
# MAGIC CORRECT (optimized)

employees.select("emp_id", "dept_id") \
    .join(broadcast(departments.select("dept_id", "dept_name")), "dept_id")

# COMMAND ------------ 

# MAGIC %md
# MAGIC Detecting performance issues
# MAGIC
# MAGIC Identify shuffle

df.explain("formatted")

# MAGIC %md
# MAGIC Look for:
# MAGIC
# MAGIC Exchange

# MAGIC %md
# MAGIC Identify broadcast

df.explain()

# MAGIC %md
# MAGIC Look for:
# MAGIC
# MAGIC BroadcastHashJoin
