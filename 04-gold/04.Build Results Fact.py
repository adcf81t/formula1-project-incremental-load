# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC
# MAGIC 1. Read silver `results` table
# MAGIC 1. Read silver `sprints` table
# MAGIC 1. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC 1. UNION `results` and `sprints`
# MAGIC 1. Derive additional columns
# MAGIC     - is_win -> Indicates that the driver own the race
# MAGIC     - is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC     - has_points -> Indicates that the driver has scored points
# MAGIC 1. Write the transformed data to gold `fact_session_results` table
# MAGIC
# MAGIC Below changes are required to implement incremental load processing
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e. filter reading from silver using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp to the silver table.
# MAGIC 4. Merge the processed data to the gold table
# MAGIC   - created_timestamp should only be populated at the time of inserting/creating the record. It should not be updated during the merge update.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Entity Relationship Diagram - Formula1 Silver Schema
# MAGIC
# MAGIC ![Formula1 Silver Data.png](../../z-course-images/formula1-silver-data-erd.png "Formula1 Silver Data.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Entity Relationship Diagram - Formula1 Gold Schema
# MAGIC
# MAGIC ![Formula1 Gold Data.png](../../z-course-images/formula1-gold-data-erd.png "Formula1 Gold Data.png")

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - `silver.results`
# MAGIC - `silver.sprints`

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
         .filter(F.col("batch_id") == v_batch_id)
         .withColumn("session_type", F.lit("RACE"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file", "batch_id", "created_timestamp", "updated_timestamp")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
 #        .filter(F.col("batch_id") == v_batch_id)
         .withColumn("session_type", F.lit("SPRINT"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file", "batch_id", "created_timestamp", "updated_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - UNION `results` and `sprints`

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add dervied columns
# MAGIC 1. is_win -> Indicates that the driver own the race
# MAGIC 1. is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC 1. has_points -> Indicates that the driver has scored points
# MAGIC

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
        .withColumn("is_win", F.col("final_position") == 1)
        .withColumn("is_podium", F.col("final_position").between(1, 3))
        .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2021"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the transformed data to the `gold` `fact_session_results` table

# COMMAND ----------

(
    fact_session_results_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

# Drop existing table to migrate schema (one-time: adds created_timestamp, updated_timestamp)
spark.sql(f"DROP TABLE IF EXISTS {target_table}")

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.driver_id = s.driver_id AND t.constructor_id = s.constructor_id AND t.session_type = s.session_type",
    columns_to_update=["grid_position", "completed_laps", "car_number", "points", "final_position", "final_position_text", "status", "is_win", "is_podium", "has_points"]
)

# COMMAND ----------

display(spark.table(target_table))
