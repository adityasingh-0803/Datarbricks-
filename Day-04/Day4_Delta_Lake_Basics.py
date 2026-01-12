# Day 4: Delta Lake Introduction
# Topics: ACID, Schema Enforcement, Delta vs Parquet

# -----------------------------------
# 1. Load CSV data
# -----------------------------------
events = spark.read.csv(
    "/FileStore/tables/sample_ecommerce.csv",
    header=True,
    inferSchema=True
)

# -----------------------------------
# 2. Convert CSV to Delta format
# -----------------------------------
events.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/events")

print("CSV converted to Delta format.")

# -----------------------------------
# 3. Create managed Delta table (PySpark)
# -----------------------------------
events.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("events_table")

print("Managed Delta table created.")

# -----------------------------------
# 4. Create Delta table using SQL
# -----------------------------------
spark.sql("""
    CREATE OR REPLACE TABLE events_delta
    USING DELTA
    AS SELECT * FROM events_table
""")

print("Delta table created using SQL.")

# -----------------------------------
# 5. Test schema enforcement
# -----------------------------------
try:
    wrong_schema_df = spark.createDataFrame(
        [("a", "b", "c")],
        ["x", "y", "z"]
    )

    wrong_schema_df.write.format("delta") \
        .mode("append") \
        .save("/delta/events")

except Exception as e:
    print("Schema enforcement error:")
    print(e)

# -----------------------------------
# 6. Handle duplicate inserts
# -----------------------------------
# Attempt to insert duplicate data
events.write.format("delta") \
    .mode("append") \
    .save("/delta/events")

print("Duplicate insert attempted (ACID guarantees consistency).")
