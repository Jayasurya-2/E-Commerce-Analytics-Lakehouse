# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema

# COMMAND ----------

bronze_schema = 'e_commerce.bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config file_path

# COMMAND ----------

users_path = '/Volumes/e_commerce/default/raw_data_files/users/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load raw data into DataFrame

# COMMAND ----------

users_df = spark.read\
                .format('csv')\
                .option('header', 'true')\
                .option('inferSchema', 'true')\
                .load(users_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add _ingest_timestamp & _source_file_name

# COMMAND ----------

from pyspark.sql.functions import *

users_df = users_df.withColumn('_ingest_timestamp', current_timestamp())\
                        .withColumn('_source_file_name', col('_metadata.file_name'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Processing - Filter Existing Bronze Table

# COMMAND ----------

bronze_table_name = f'{bronze_schema}.bronze_users'

# COMMAND ----------

# Check whether the bronze table already exists and has data
# If YES -> read the highest event_id already loaded (watermark)
# If NO -> set watermark to -1 so every row in the csv is treated as new

from delta.tables import DeltaTable

if spark.catalog.tableExists(bronze_table_name):
    max_event_id = spark.sql(
        f'SELECT COALESCE(MAX(user_id), -1) AS max_id FROM {bronze_table_name}'
    ).collect()[0]['max_id']
else :
    max_event_id = -1

print(f'Bronze watermark -> max event_id already loaded : {max_event_id}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter DataFrame to new rows only

# COMMAND ----------

users_df = users_df.filter(col('user_id') > max_event_id)

new_row_count = users_df.count()
print(f'New rows to ingest : {new_row_count}')

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Layer

# COMMAND ----------

# Append New Rows to Bronze Delta Table

if new_row_count > 0 :
    users_df.write\
                .format('delta')\
                .mode('append')\
                .option('mergeSchema', 'true')\
                .saveAsTable(bronze_table_name)
    
    print(f'✅ {new_row_count:,} new rows appended to {bronze_table_name}')
else :
    print(f'ℹ️ No new data found. Bronze table is already up-to-date.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Post-Load Row Count & Ingest Summary
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {bronze_table_name}").show()
spark.sql(f"""
    SELECT DATE(_ingest_timestamp) AS ingest_date,
           COUNT(*)                AS rows_ingested,
           MIN(user_id)      AS min_event_id,
           MAX(user_id)      AS max_event_id
    FROM   {bronze_table_name}
    GROUP  BY 1 ORDER BY 1 DESC LIMIT 10
""").show(truncate=False)