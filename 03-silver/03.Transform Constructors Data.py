# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Constructors Data
# MAGIC
# MAGIC 1. Read bronze `constructors` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC 1. Rename columns to make them more meaningful (`name` → `constructor_name`)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `nationality` to Title Case
# MAGIC 1. Write the transformed data to silver `constructors` table
# MAGIC
# MAGIC Below changes are required to implement incremental load processing
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e. filter reading from bronze using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp and batch_id to the silver table.
# MAGIC 4. Merge the processed data to the silver table
# MAGIC   - created_timestamp should only be populated at the time of inserting/creating the record. It should not be updated during the merge update.
# MAGIC   - Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Entity Relationship Diagram - Formula1 Bronze Schema
# MAGIC
# MAGIC ![Formula1 Raw Data.png](../../z-course-images/formula1-raw-data-erd.png "Formula1 Raw Data.png")

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `constructors` table

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC - Rename columns to make them more meaningful (`name` → `constructor_name`)

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transform values of column `nationality` to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write the transformed data to silver `constructors` table

# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

# Add batch_id column required by write_to_silver merge condition
constructors_with_batch_df = constructors_final_df.withColumn("batch_id", F.lit(v_batch_id))

# Drop existing table to migrate schema (one-time: adds created_timestamp, updated_timestamp, batch_id)
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

write_to_silver(input_df=constructors_with_batch_df, 
                target_table=silver_table,
                merge_condition="t.constructor_id = s.constructor_id",
                columns_to_update=["constructor_name", "nationality", "ingestion_timestamp", "source_file", "batch_id"])

# COMMAND ----------

display(spark.table(silver_table))
