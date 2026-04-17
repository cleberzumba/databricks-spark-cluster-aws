# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 7: UDFs and Built-in Functions
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Regular UDFs** - Python functions wrapped as Spark UDFs
# MAGIC - **Pandas UDFs** - Vectorized UDFs (faster, uses Arrow)
# MAGIC - **When to use UDFs** vs built-in functions
# MAGIC - **Stateless vs Stateful UDFs** 
# MAGIC - Built-in functions (concat, when, coalesce, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, pandas_udf, lit, when, concat, concat_ws,
    coalesce, nvl, regexp_replace, trim, upper, lower,
    length, substring
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
import pandas as pd

# Sample DataFrame
data = [
    (1, "john.doe@email.com", "New York", 75000),
    (2, "jane.smith@email.com", "London", 65000),
    (3, "bob.johnson@email.com", "Paris", 80000),
    (4, None, "Berlin", None),
]
df = spark.createDataFrame(data, ["id", "email", "city", "salary"])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating a Basic UDF
# MAGIC
# MAGIC ### When to Use UDFs:
# MAGIC - Custom logic not available in built-in functions
# MAGIC - Complex transformations
# MAGIC - **AVOID if built-in function exists** (built-ins are faster!)

# COMMAND ----------

# Define Python function
def extract_username(email):
    """Extract username from email address"""
    if email is None:
        return None
    return email.split("@")[0]

# Create UDF (METHOD 1: using udf function)
extract_username_udf = udf(extract_username, StringType())

# Apply UDF
result = df.withColumn("username", extract_username_udf(col("email")))
result.show()

# COMMAND ----------

# METHOD 2: Using decorator
@udf(returnType=StringType())
def extract_domain(email):
    """Extract domain from email"""
    if email is None:
        return None
    return email.split("@")[1] if "@" in email else None

# Apply
df.withColumn("domain", extract_domain(col("email"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UDF with Multiple Parameters

# COMMAND ----------

# UDF with multiple inputs
def calculate_tax(salary, tax_rate):
    """Calculate tax amount"""
    if salary is None:
        return None
    return salary * tax_rate

calculate_tax_udf = udf(calculate_tax, DoubleType())

# Apply with constant tax rate
df.withColumn("tax", calculate_tax_udf(col("salary"), lit(0.2))).show()

# Apply with variable tax rate
df_with_rate = df.withColumn("tax_rate",
    when(col("salary") > 70000, 0.3)
    .otherwise(0.2)
)
df_with_rate.withColumn("tax", calculate_tax_udf(col("salary"), col("tax_rate"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pandas UDFs (Vectorized UDFs) - More Efficient!
# MAGIC
# MAGIC ### Advantages:
# MAGIC - Much faster than regular UDFs
# MAGIC - Uses Apache Arrow for efficient data transfer
# MAGIC - Operates on pandas Series/DataFrames
# MAGIC
# MAGIC ### Types:
# MAGIC - Series to Series
# MAGIC - Iterator of Series to Iterator of Series
# MAGIC - Iterator of Multiple Series to Iterator of Series

# COMMAND ----------

# Pandas UDF - Series to Series
@pandas_udf(StringType())
def extract_username_pandas(emails: pd.Series) -> pd.Series:
    """Extract username using pandas (vectorized)"""
    return emails.str.split("@").str[0]

# Apply
df.withColumn("username", extract_username_pandas(col("email"))).show()

# COMMAND ----------

# Pandas UDF with scalar return
@pandas_udf(DoubleType())
def calculate_bonus(salaries: pd.Series) -> pd.Series:
    """Calculate bonus (10% of salary)"""
    return salaries * 0.1

df.withColumn("bonus", calculate_bonus(col("salary"))).show()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# Validates each transaction independently
@udf(returnType=BooleanType())
def validate_transaction(amount, city):
    """
    Validate transaction without state
    - No history needed
    - Each transaction validated independently
    """
    if amount is None or city is None:
        return False

    # Business rules that don't need previous transactions
    if amount < 0:
        return False
    if amount > 10000 and city not in ["New York", "London"]:
        return False

    return True

# Apply stateless validation
df.withColumn("is_valid", validate_transaction(col("salary"), col("city"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Built-in Functions You Must Know
# MAGIC
# MAGIC ### Always prefer built-in functions over UDFs!
# MAGIC - Much faster (optimized by Catalyst)
# MAGIC - No serialization overhead
# MAGIC - Better performance

# COMMAND ----------

# String functions
string_df = spark.createDataFrame([
    (1, "  John Doe  ", "ENGINEER"),
    (2, "jane smith", "manager"),
], ["id", "name", "role"])

result = string_df.select(
    col("id"),
    # Trim whitespace
    trim(col("name")).alias("trimmed"),
    # Upper/Lower case
    upper(col("name")).alias("upper_name"),
    lower(col("role")).alias("lower_role"),
    # Length
    length(col("name")).alias("name_length"),
    # Substring
    substring(col("name"), 1, 4).alias("first_4_chars"),
    # Concat
    concat(col("name"), lit(" - "), col("role")).alias("combined")
)
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conditional Functions - when() and coalesce()

# COMMAND ----------

# when() - SQL CASE WHEN equivalent
df.withColumn("salary_category",
    when(col("salary") > 75000, "High")
    .when(col("salary") > 65000, "Medium")
    .when(col("salary").isNull(), "Unknown")
    .otherwise("Low")
).show()

# coalesce() - Return first non-null value
df.withColumn("salary_or_default",
    coalesce(col("salary"), lit(50000))
).show()

# nvl() - Same as coalesce for 2 values
df.withColumn("email_or_na",
    coalesce(col("email"), lit("N/A"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. concat() and concat_ws()

# COMMAND ----------

# concat - Concatenate columns
test_df = spark.createDataFrame([
    (1, "John", "Doe", "Engineer"),
    (2, "Jane", "Smith", None),
], ["id", "first", "last", "role"])

# concat - NULLs result in NULL
test_df.withColumn("full_info",
    concat(col("first"), lit(" "), col("last"), lit(" - "), col("role"))
).show()

# concat_ws - With separator, skips NULLs
test_df.withColumn("full_info",
    concat_ws(" | ", col("first"), col("last"), col("role"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. regexp_replace() - Regular Expressions

# COMMAND ----------

# Replace patterns with regex
email_df = spark.createDataFrame([
    (1, "john.doe@company.com"),
    (2, "jane_smith@company.com"),
], ["id", "email"])

# Replace dots with underscores
email_df.withColumn("email_clean",
    regexp_replace(col("email"), r"\.", "_")
).show()

# Extract domain
email_df.withColumn("domain",
    regexp_replace(col("email"), r".*@", "")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Registering UDFs for SQL Use

# COMMAND ----------

# Register UDF for SQL
def calculate_bonus_py(salary):
    return salary * 0.1 if salary else None

spark.udf.register("calculate_bonus_sql", calculate_bonus_py, DoubleType())

# Create temp view
df.createOrReplaceTempView("employees")

# Use UDF in SQL
spark.sql("""
    SELECT
        id,
        salary,
        calculate_bonus_sql(salary) as bonus
    FROM employees
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. UDF Performance Considerations
# MAGIC
# MAGIC ### Performance Ranking:
# MAGIC 1. **Built-in functions** - Always use if available!
# MAGIC 2. **Pandas UDFs** - Vectorized, uses Arrow
# MAGIC 3. **Regular Python UDFs** - Row-by-row, slowest
# MAGIC
# MAGIC ### Why UDFs are Slower:
# MAGIC - Serialization overhead (Python ↔ JVM)
# MAGIC - Can't be optimized by Catalyst
# MAGIC - No predicate pushdown

# COMMAND ----------

# EXAMPLE: Avoid UDF when built-in exists!

# BAD - Using UDF for simple transformation
@udf(returnType=StringType())
def to_upper_udf(text):
    return text.upper() if text else None

df.withColumn("city_upper", to_upper_udf(col("city"))).show()

# GOOD - Use built-in function
df.withColumn("city_upper", upper(col("city"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Pandas UDF for Aggregations

# COMMAND ----------

# Group aggregate Pandas UDF
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf(DoubleType())
def mean_salary_pandas(salaries: pd.Series) -> float:
    """Calculate mean salary using pandas"""
    return salaries.mean()

# Use in groupBy
df.groupBy("city").agg(
    mean_salary_pandas(col("salary")).alias("avg_salary_pandas")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. EXAMPLE - When to Use UDFs

# COMMAND ----------

# "Developer wants to ensure streaming job is stateless for each transaction
# and can scale horizontally without maintaining customer history."

# CORRECT: Stateless UDF
@udf(returnType=BooleanType())
def validate_transaction_stateless(amount, product, customer_id):
    """
    Validate transaction without state
    - Each transaction independent
    - No previous history needed
    - Can scale horizontally
    """
    # Validation rules that don't need state
    if amount is None or amount <= 0:
        return False
    if product is None or len(product) == 0:
        return False

    # Business rules
    if amount > 5000:  # Flag high-value
        return True

    return True

# Example usage
transactions = spark.createDataFrame([
    (1, 100.0, "Widget", "C001"),
    (2, 6000.0, "Gadget", "C002"),
    (3, -50.0, "Tool", "C003"),
], ["id", "amount", "product", "customer_id"])

transactions.withColumn("is_valid",
    validate_transaction_stateless(
        col("amount"),
        col("product"),
        col("customer_id")
    )
).show()

# COMMAND ----------

