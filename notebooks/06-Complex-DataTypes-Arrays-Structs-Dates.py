# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 6: Complex Data Types - Arrays, Structs, Date/Time
# MAGIC
# MAGIC ### Arrays:
# MAGIC - `explode()` -  Explode array into rows
# MAGIC - `array()` - Create array column
# MAGIC - `array_contains()` - Check if array contains value
# MAGIC - `size()` - Get array length
# MAGIC - `split()` - Split string into array
# MAGIC
# MAGIC ### Structs:
# MAGIC - `struct()` - Create struct column
# MAGIC - Access with dot notation: `col("struct.field")`
# MAGIC - `getField()` - Access struct field
# MAGIC
# MAGIC ### Date/Time:
# MAGIC - `current_date()`, `current_timestamp()` - Current date/time
# MAGIC - `to_date()`, `to_timestamp()` - Convert to date/timestamp
# MAGIC - `from_unixtime()` - Unix epoch to date
# MAGIC - `unix_timestamp()` - Date to Unix epoch
# MAGIC - `year()`, `month()`, `dayofmonth()` - Extract components
# MAGIC - `date_add()`, `date_sub()` - Date arithmetic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    # Array functions
    explode, array, array_contains, size, split, collect_list,
    # Struct functions
    struct, col,
    # Date/Time functions
    current_date, current_timestamp, to_date, to_timestamp,
    from_unixtime, unix_timestamp, date_format,
    year, month, dayofmonth, dayofweek, dayofyear, weekofyear,
    hour, minute, second,
    date_add, date_sub, datediff, months_between,
    # Other
    lit, when, upper, lower, concat
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Arrays - Creating and Basic Operations

# COMMAND ----------

# Create DataFrame with arrays
data = [
    (1, "John", ["Python", "Scala", "Java"]),
    (2, "Jane", ["Python", "R"]),
    (3, "Bob", ["Java", "Scala"]),
]

df_arrays = spark.createDataFrame(data, ["id", "name", "skills"])
df_arrays.show(truncate=False)
df_arrays.printSchema()

# COMMAND ----------

# Create array from columns
data2 = [(1, "John", 85, 90, 78)]
df_scores = spark.createDataFrame(data2, ["id", "name", "math", "english", "science"])

# Create array column
df_with_array = df_scores.withColumn(
    "all_scores",
    array(col("math"), col("english"), col("science"))
)
df_with_array.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. explode()
# MAGIC
# MAGIC - Converts array elements into separate rows
# MAGIC - One row per array element
# MAGIC - NULL arrays become zero rows (removed)

# COMMAND ----------

# Explode array into rows
df_exploded = df_arrays.select(
    col("id"),
    col("name"),
    explode(col("skills")).alias("skill")
)
df_exploded.show()

# Notice: John had 3 skills, now has 3 rows

# COMMAND ----------

# Explode with other columns
df_arrays.select(
    col("name"),
    explode(col("skills")).alias("individual_skill")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Array Functions
# MAGIC
# MAGIC ### Common Operations:

# COMMAND ----------

# array_contains - Check if array contains value
df_arrays.filter(array_contains(col("skills"), "Python")).show()

# size - Get array length
df_arrays.withColumn("num_skills", size(col("skills"))).show()

# Access array element by index (0-based)
df_arrays.withColumn("first_skill", col("skills")[0]).show()
df_arrays.withColumn("second_skill", col("skills")[1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. split() - String to Array
# MAGIC
# MAGIC ### Common Pattern:
# MAGIC - Split string by delimiter into array
# MAGIC - Often followed by explode()

# COMMAND ----------

# Create DataFrame with delimited strings
data_str = [
    (1, "John", "Python,Scala,Java"),
    (2, "Jane", "Python,R"),
]
df_str = spark.createDataFrame(data_str, ["id", "name", "skills_str"])

# Split string into array
df_split = df_str.withColumn("skills_array", split(col("skills_str"), ","))
df_split.show(truncate=False)

# Split and explode in one go
df_str.select(
    col("name"),
    explode(split(col("skills_str"), ",")).alias("skill")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Structs - Nested Structures

# COMMAND ----------

# Create DataFrame with struct
data_struct = [
    (1, "John", ("New York", "USA", "10001")),
    (2, "Jane", ("London", "UK", "SW1A 1AA")),
]

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zipcode", StringType(), True)
])

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", address_schema, True)
])

df_struct = spark.createDataFrame(data_struct, schema)
df_struct.show(truncate=False)
df_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Accessing Struct Fields
# MAGIC
# MAGIC ### Two Methods:
# MAGIC - Dot notation: `col("struct.field")`
# MAGIC - getField() function

# COMMAND ----------

# METHOD 1: Dot notation (most common)
df_struct.select(
    col("name"),
    col("address.city"),
    col("address.country"),
    col("address.zipcode")
).show()

# METHOD 2: getField()
df_struct.select(
    col("name"),
    col("address").getField("city").alias("city")
).show()

# METHOD 3: Bracket notation (alternative to dot notation)
df_struct.select(
    col("name"),
    col("address")["city"].alias("city"),
    col("address")["country"].alias("country")
).show()

# METHOD 4: Using wildcard to expand all struct fields
df_struct.select("name", "address.*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5. Advanced Struct Manipulation
# MAGIC
# MAGIC - **Adding new fields** to existing struct
# MAGIC - **Renaming fields** within struct
# MAGIC - **Modifying specific fields** while keeping others
# MAGIC
# MAGIC You CANNOT modify a struct in place - you must RECREATE it!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Add a New Field to Existing Struct

# COMMAND ----------

# Add "country_code" field to address struct
df_with_new_field = df_struct.withColumn(
    "address",
    struct(
        col("address.street").alias("street"),      # Keep existing
        col("address.city").alias("city"),          # Keep existing
        col("address.zipcode").alias("zipcode"),    # Keep existing
        lit("US").alias("country_code")             # NEW field
    )
)

df_with_new_field.show(truncate=False)
df_with_new_field.printSchema()

# COMMAND ----------

# Alternative: Use address.* to keep all existing fields, then add new ones
df_with_new_field_v2 = df_struct.withColumn(
    "address",
    struct(
        col("address.*"),                           # Keep ALL existing fields
        lit("US").alias("country_code")             # Add new field
    )
)

df_with_new_field_v2.show(truncate=False)
df_with_new_field_v2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Rename a Field Within Struct

# COMMAND ----------

# Rename "zipcode" to "postal_code" in address struct
df_renamed_field = df_struct.withColumn(
    "address",
    struct(
        col("address.street").alias("street"),
        col("address.city").alias("city"),
        col("address.zipcode").alias("postal_code")  # Renamed!
    )
)

df_renamed_field.show(truncate=False)
df_renamed_field.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Modify Specific Field While Keeping Others

# COMMAND ----------

# Make city uppercase while keeping other fields unchanged
df_modified = df_struct.withColumn(
    "address",
    struct(
        col("address.street").alias("street"),
        upper(col("address.city")).alias("city"),    # Modified!
        col("address.zipcode").alias("zipcode")
    )
)

df_modified.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4: Conditional Field Values in Struct

# COMMAND ----------

# Add "region" field based on city
df_with_region = df_struct.withColumn(
    "address",
    struct(
        col("address.*"),
        when(col("address.city") == "New York", "Northeast")
        .when(col("address.city") == "London", "Europe")
        .otherwise("Other")
        .alias("region")
    )
)

df_with_region.show(truncate=False)
df_with_region.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### All Three Access Methods - Side by Side Comparison

# COMMAND ----------

# Compare all three syntaxes (they do the same thing!)
print("METHOD 1: Dot notation")
df_struct.select(col("address.city")).show()

print("METHOD 2: Bracket notation")
df_struct.select(col("address")["city"]).show()

print("METHOD 3: getField()")
df_struct.select(col("address").getField("city")).show()

# MAGIC %md
# MAGIC **When to use each:**
# MAGIC - **Dot notation**: Most readable, best for static field names
# MAGIC - **Bracket notation**: Useful when field names are dynamic or contain special characters
# MAGIC - **getField()**: Useful in complex expressions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```python
# MAGIC # WRONG - Can't modify struct in place
# MAGIC df.withColumn("address.city", lit("New York"))  # ERROR!
# MAGIC
# MAGIC # CORRECT - Must recreate the struct
# MAGIC df.withColumn("address",
# MAGIC     struct(
# MAGIC         lit("New York").alias("city"),
# MAGIC         col("address.street").alias("street"),
# MAGIC         col("address.zipcode").alias("zipcode")
# MAGIC     )
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Creating Structs from Columns

# COMMAND ----------

# Create simple DataFrame
simple_df = spark.createDataFrame([
    (1, "John", "New York", "USA"),
    (2, "Jane", "London", "UK"),
], ["id", "name", "city", "country"])

# Create struct column
df_with_struct = simple_df.withColumn(
    "address",
    struct(col("city"), col("country"))
)
df_with_struct.show(truncate=False)
df_with_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Date and Time - Current Values

# COMMAND ----------

# Get current date and timestamp
df_dates = spark.range(1).select(
    current_date().alias("current_date"),
    current_timestamp().alias("current_timestamp")
)
df_dates.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Converting to Date/Timestamp
# MAGIC
# MAGIC ### Common Patterns:

# COMMAND ----------

# Sample data with date strings
date_data = [
    (1, "2024-01-15", "2024-01-15 10:30:00"),
    (2, "2024-02-20", "2024-02-20 14:45:30"),
    (3, "2024-03-25", "2024-03-25 09:15:45"),
]
df_date_strings = spark.createDataFrame(date_data, ["id", "date_str", "timestamp_str"])

# Convert string to date
df_converted = df_date_strings.withColumn("date", to_date(col("date_str")))

# Convert string to timestamp
df_converted = df_converted.withColumn("timestamp", to_timestamp(col("timestamp_str")))

df_converted.show(truncate=False)
df_converted.printSchema()

# COMMAND ----------

# Convert with specific format
df_custom_format = spark.createDataFrame([
    (1, "15/01/2024"),
    (2, "20/02/2024"),
], ["id", "date_str"])

df_custom_format.withColumn(
    "date",
    to_date(col("date_str"), "dd/MM/yyyy")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Unix Timestamp Conversion
# MAGIC
# MAGIC - Unix epoch: seconds since 1970-01-01 00:00:00 UTC
# MAGIC - `from_unixtime()` - Unix timestamp to date string
# MAGIC - `unix_timestamp()` - Date to Unix timestamp

# COMMAND ----------

# Sample data with Unix timestamps
unix_data = [
    (1, 1705334400),  # 2024-01-15 12:00:00 UTC
    (2, 1708444800),  # 2024-02-20 12:00:00 UTC
]
df_unix = spark.createDataFrame(unix_data, ["id", "unix_time"])

# Convert Unix timestamp to date string
df_unix.withColumn(
    "date_string",
    from_unixtime(col("unix_time"))
).show(truncate=False)

# Convert with custom format
df_unix.withColumn(
    "formatted_date",
    from_unixtime(col("unix_time"), "yyyy-MM-dd HH:mm:ss")
).show(truncate=False)

# COMMAND ----------

# Convert date to Unix timestamp
df_date = spark.createDataFrame([
    (1, "2024-01-15 12:00:00"),
    (2, "2024-02-20 12:00:00"),
], ["id", "date_str"])

df_date.withColumn(
    "unix_timestamp",
    unix_timestamp(col("date_str"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Extracting Date Components
# MAGIC
# MAGIC - `year()` - Extract year
# MAGIC - `month()` - Extract month (1-12)
# MAGIC - `dayofmonth()` - Extract day (1-31)
# MAGIC - `dayofweek()` - Day of week (1=Sunday, 7=Saturday)
# MAGIC - `hour()`, `minute()`, `second()` - Time components

# COMMAND ----------

# Create sample data
sample_dates = spark.createDataFrame([
    (1, "2024-01-15 14:30:45"),
    (2, "2024-06-20 09:15:30"),
], ["id", "timestamp_str"])

sample_dates = sample_dates.withColumn("ts", to_timestamp(col("timestamp_str")))

# Extract all components
result = sample_dates.select(
    col("ts"),
    year(col("ts")).alias("year"),
    month(col("ts")).alias("month"),
    dayofmonth(col("ts")).alias("day"),
    dayofweek(col("ts")).alias("day_of_week"),
    dayofyear(col("ts")).alias("day_of_year"),
    weekofyear(col("ts")).alias("week_of_year"),
    hour(col("ts")).alias("hour"),
    minute(col("ts")).alias("minute"),
    second(col("ts")).alias("second")
)
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Date Arithmetic
# MAGIC
# MAGIC ### Common Operations:

# COMMAND ----------

# Sample data
df_dates = spark.createDataFrame([
    (1, "2024-01-15"),
    (2, "2024-06-20"),
], ["id", "date_str"])

df_dates = df_dates.withColumn("date", to_date(col("date_str")))

# Date arithmetic
result = df_dates.select(
    col("date"),
    date_add(col("date"), 7).alias("plus_7_days"),
    date_sub(col("date"), 7).alias("minus_7_days"),
    date_add(col("date"), 30).alias("plus_30_days")
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Date Differences

# COMMAND ----------

# Sample data with two dates
df_two_dates = spark.createDataFrame([
    (1, "2024-01-01", "2024-01-15"),
    (2, "2024-01-01", "2024-06-30"),
], ["id", "start_date_str", "end_date_str"])

df_two_dates = df_two_dates \
    .withColumn("start_date", to_date(col("start_date_str"))) \
    .withColumn("end_date", to_date(col("end_date_str")))

# Calculate differences
result = df_two_dates.select(
    col("start_date"),
    col("end_date"),
    datediff(col("end_date"), col("start_date")).alias("days_between"),
    months_between(col("end_date"), col("start_date")).alias("months_between")
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Date Formatting
# MAGIC
# MAGIC ### Format Patterns:
# MAGIC - `yyyy` - 4-digit year
# MAGIC - `MM` - 2-digit month
# MAGIC - `dd` - 2-digit day
# MAGIC - `HH` - 2-digit hour (24h)
# MAGIC - `mm` - 2-digit minute
# MAGIC - `ss` - 2-digit second

# COMMAND ----------

df_sample = spark.createDataFrame([
    (1, "2024-01-15 14:30:45"),
], ["id", "timestamp_str"])

df_sample = df_sample.withColumn("ts", to_timestamp(col("timestamp_str")))

# Format in different ways
result = df_sample.select(
    col("ts"),
    date_format(col("ts"), "yyyy-MM-dd").alias("date_only"),
    date_format(col("ts"), "MM/dd/yyyy").alias("us_format"),
    date_format(col("ts"), "yyyy-MM-dd HH:mm:ss").alias("full_timestamp"),
    date_format(col("ts"), "EEEE, MMMM dd, yyyy").alias("long_format")
)
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# Scenario: Process nested data with dates
exam_data = [
    (1, "2024-01-15", ["Python", "Scala"], {"city": "New York", "zip": "10001"}),
    (2, "2024-02-20", ["Java", "R"], {"city": "London", "zip": "SW1"}),
]

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("join_date", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True)
])

df_exam = spark.createDataFrame(exam_data, schema)

# Complex transformation:
# 1. Convert date string to date
# 2. Extract year and month
# 3. Explode skills array
# 4. Access struct fields

result = df_exam \
    .withColumn("date", to_date(col("join_date"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .select(
        col("id"),
        col("date"),
        col("year"),
        col("month"),
        explode(col("skills")).alias("skill"),
        col("location.city").alias("city")
    )

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Arrays
# MAGIC ```python
# MAGIC # explode() - MOST IMPORTANT!
# MAGIC df.select(explode(col("array_col")).alias("element"))
# MAGIC
# MAGIC # Create array
# MAGIC df.withColumn("arr", array(col("a"), col("b"), col("c")))
# MAGIC
# MAGIC # Array operations
# MAGIC array_contains(col("arr"), "value")  # Check contains
# MAGIC size(col("arr"))  # Array length
# MAGIC col("arr")[0]  # Access by index
# MAGIC split(col("str"), ",")  # String to array
# MAGIC ```
# MAGIC
# MAGIC ### Structs
# MAGIC ```python
# MAGIC # Access struct fields (THREE methods - all equivalent!)
# MAGIC col("struct_col.field_name")              # Dot notation (most common)
# MAGIC col("struct_col")["field_name"]           # Bracket notation
# MAGIC col("struct_col").getField("field_name")  # getField() method
# MAGIC
# MAGIC # Create struct
# MAGIC struct(col("a"), col("b"))
# MAGIC
# MAGIC # Expand all fields
# MAGIC df.select("struct_col.*")
# MAGIC
# MAGIC # Add new field to existing struct
# MAGIC df.withColumn("address",
# MAGIC     struct(
# MAGIC         col("address.*"),                  # Keep all existing
# MAGIC         lit("USA").alias("country")        # Add new field
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # Rename field within struct
# MAGIC df.withColumn("address",
# MAGIC     struct(
# MAGIC         col("address.street").alias("street"),
# MAGIC         col("address.city").alias("city"),
# MAGIC         col("address.zip").alias("postal_code")  # Renamed!
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # Modify field value within struct
# MAGIC df.withColumn("address",
# MAGIC     struct(
# MAGIC         col("address.*"),
# MAGIC         upper(col("address.city")).alias("city")  # Modified!
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # CRITICAL: Can't modify struct in place!
# MAGIC # df.withColumn("struct.field", value)  # ERROR!
# MAGIC # Must recreate entire struct
# MAGIC ```
# MAGIC
# MAGIC ### Date/Time 
# MAGIC ```python
# MAGIC # Unix timestamp conversion
# MAGIC from_unixtime(col("unix_ts"))  # Unix to date string
# MAGIC unix_timestamp(col("date"))  # Date to Unix
# MAGIC
# MAGIC # Extract components
# MAGIC year(col("date"))
# MAGIC month(col("date"))
# MAGIC dayofmonth(col("date"))
# MAGIC
# MAGIC # Convert formats
# MAGIC to_date(col("str"))
# MAGIC to_timestamp(col("str"))
# MAGIC date_format(col("date"), "yyyy-MM-dd")
# MAGIC
# MAGIC # Date arithmetic
# MAGIC date_add(col("date"), 7)  # Add days
# MAGIC date_sub(col("date"), 7)  # Subtract days
# MAGIC datediff(col("end"), col("start"))  # Days between
# MAGIC ```
# MAGIC
# MAGIC ### EXAMPLE:
# MAGIC ```python
# MAGIC # Pattern 1: Explode array
# MAGIC df.select(col("id"), explode(col("tags")).alias("tag"))
# MAGIC
# MAGIC # Pattern 2: Split and explode
# MAGIC df.select(explode(split(col("csv_str"), ",")).alias("value"))
# MAGIC
# MAGIC # Pattern 3: Extract date components
# MAGIC df.withColumn("year", year(col("date"))) \
# MAGIC   .withColumn("month", month(col("date")))
# MAGIC
# MAGIC # Pattern 4: Access nested struct (3 ways!)
# MAGIC df.select(col("address.city"), col("address.zip"))      # Dot notation
# MAGIC df.select(col("address")["city"], col("address")["zip"])  # Bracket notation
# MAGIC df.select(col("address").getField("city"))             # getField()
# MAGIC
# MAGIC # Pattern 5: Add field to struct
# MAGIC df.withColumn("address",
# MAGIC     struct(col("address.*"), lit("USA").alias("country")))
# MAGIC ```
