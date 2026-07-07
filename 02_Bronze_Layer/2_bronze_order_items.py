# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema
# MAGIC

# COMMAND ----------

bronze_schema = 'e_commerce.bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config file_path

# COMMAND ----------

order_items_path = '/Volumes/e_commerce/default/raw_data_files/order_items/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load raw data into DataFrame

# COMMAND ----------

order_items_df = spark.read\
                .format('csv')\
                .option('header', 'true')\
                .option('inferSchema', 'true')\
                .load(order_items_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add _ingest_timestamp & _source_file_name

# COMMAND ----------

from pyspark.sql.functions import *

order_items_df = order_items_df.withColumn('_ingest_timestamp', current_timestamp())\
                        .withColumn('_source_file_name', col('_metadata.file_name'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Processing - Filter Existing Bronze Table

# COMMAND ----------

bronze_table = f'{bronze_schema}.bronze_order_items'

# COMMAND ----------

# Check whether the bronze table already exists and has data
# If YES -> read the highest event_id already loaded (watermark)
# If NO -> set watermark to -1 so every row in the csv is treated as new

from delta.tables import DeltaTable

if spark.catalog.tableExists(bronze_table):
    max_id = spark.sql(
        f"SELECT COALESCE(MAX(order_item_id), -1) AS max_id FROM {bronze_table}"
    ).collect()[0]['max_id']
else:
    max_id = -1

print(f"Bronze watermark → max order_item_id already loaded : {max_id:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter DataFrame to new rows only

# COMMAND ----------

order_items_df = order_items_df.filter(col('order_item_id') > max_id)
new_row_count = order_items_df.count()
print(f"New rows to ingest : {new_row_count:,}")

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Layer

# COMMAND ----------

# Append New Rows to Bronze Delta Table
if new_row_count > 0:
    (
        order_items_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(bronze_table)
    )
    print(f"✅  {new_row_count:,} rows appended to {bronze_table}")
else:
    print("ℹ️   No new rows. Bronze table is already current.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Post-Load Row Count & Ingest Summary
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {bronze_table}").show()
spark.sql(f"""
    SELECT DATE(_ingest_timestamp) AS ingest_date,
           COUNT(*)                AS rows_ingested,
           MIN(order_item_id)      AS min_id,
           MAX(order_item_id)      AS max_id
    FROM   {bronze_table}
    GROUP  BY 1 ORDER BY 1 DESC LIMIT 10
""").show(truncate=False)