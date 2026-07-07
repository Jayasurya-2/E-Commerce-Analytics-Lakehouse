# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_events'
silver_table = f'{silver_schema}.silver_events'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import * 
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Key

# COMMAND ----------

Merge_Key = 'event_id'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read last processed TimeStamp from Silver
# MAGIC WaterMark Stratergy

# COMMAND ----------

EPOCH_WATERMARK = '1900-01-01 00:00:00'

if spark.catalog.tableExists(silver_table):
    watermark_row = spark.sql(f'''
                              SELECT coalesce(MAX(_ingest_timestamp), CAST('{EPOCH_WATERMARK}' AS TIMESTAMP)) AS last_ts
                              FROM {silver_table}
                              ''').collect()[0]
    last_processed_ts = watermark_row['last_ts']
else:
    last_processed_ts = EPOCH_WATERMARK

print(f'Silver WaterMark -> last_ingest_timestamp processed : {last_processed_ts}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Incremental Batch from Bronze

# COMMAND ----------

# Pull New / Updated Rows from Bronze Delta Table
# We push the watermark filter into bronze delta table so that we can avoid reading all the data via 
# _ingest_timestamp
# This avoids a FULL TABLE SCAN ON EVERY PIPELINE RUN

silver_events = spark.sql(f'''
                            SELECT *
                            FROM {bronze_table}
                            WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
                            ''')

batch_row_count = silver_events.count()
print(f'Bronze rows in this batch (pre_dedup): {batch_row_count}')                            

if batch_row_count == 0:
    print('No new data in bronze table')
    dbutils.notebook.exit('NO NEW DATA IN BRONZE TABLE')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy(Merge_Key).orderBy(col("event_timestamp").desc())

silver_events = (
    silver_events.withColumn("rank", row_number().over(dedup_window))
    .filter(col("rank") == 1)
    .drop("rank")
)

print(f'Duplicates Removed : {(batch_row_count - silver_events.count()):,}')
print(f'Batch rows after DeDuplication : {silver_events.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Numeric Columns

# COMMAND ----------

silver_events = (
    silver_events
    .withColumn('duration_seconds', col('duration_seconds').cast('double'))
    .withColumn('scroll_depth_pct', col('scroll_depth_pct').cast('double'))
    .withColumn('click_x', col('click_x').cast('double'))
    .withColumn('click_y', col('click_y').cast('double'))
    .withColumn('conversion_value', col('conversion_value').cast('double'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Handling - String Columns

# COMMAND ----------

silver_events = silver_events.fillna({
    "session_id": "Unknown",
    "operating_system": "Unknown",
    "device_type": "Unknown",
    "country": "Unknown",
    "city": "Unknown",
    "page_url": "/unknown",
    "referrer_source": "default",
    "browser": "default",
    "ab_test_variant": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Handling - Numerice Columns

# COMMAND ----------

duration_median = silver_events.approxQuantile('duration_seconds', [0.5], 0.01)[0]
print(f'Median duration_seconds (approx) : {duration_median}')

# COMMAND ----------

silver_events = silver_events.fillna(
    {
        'duration_seconds': duration_median,
        'click_x': 0,
        'click_y': 0,
        'conversion_value': 0.0,
        'scroll_depth_pct': 0.0,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search Query Handling

# COMMAND ----------

# Normalise search_query - only meaningful for search events
# For non-search events set to N/A so downstream aggregations

silver_events = silver_events.withColumn('search_query', coalesce(
    when(col('event_type') == 'search', col('search_query')),
    lit('N/A')
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolea Normalisation - is_bot

# COMMAND ----------

silver_events = silver_events.withColumn(
    "is_bot",
    when(lower(trim(col("is_bot"))).isin("yes", "y", "true"), lit(True))
    .when(lower(trim(col("is_bot"))).isin("no", "n", "false"), lit(False))
    .otherwise(lit(False)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Normalisation

# COMMAND ----------

silver_events = (
    silver_events
    .withColumn('device_type',   initcap(col('device_type')))
    .withColumn('search_query',  lower(col('search_query')))   
    .withColumn('browser',       initcap(trim(col('browser'))))
    .withColumn('event_type',    lower(trim(col('event_type'))))
    .withColumn('referrer_source', lower(trim(col('referrer_source'))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_events = (
    silver_events
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source', lit('bronze_events'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre - Write Data Quaity Checks

# COMMAND ----------

print('*' * 10)
print(' Silver Events - Data Quality Report (CURRENT BATCH)')
print('*' * 10)

total_rows = silver_events.count()

silver_events_checks = {
    'Null event_id' : silver_events.filter(col('event_id').isNull()).count(),
    'Null user_id' : silver_events.filter(col('user_id').isNull()).count(),
    'Null event_timestamp' : silver_events.filter(col('event_timestamp').isNull()).count(),
    'Null session_id' : silver_events.filter(col('session_id').isNull()).count(),
    'Negative duration_seconds' : silver_events.filter(col('duration_seconds') < 0).count(),
    'scroll_depth_pct > 100' : silver_events.filter(col('scroll_depth_pct') > 100).count(),
    'Null Conversion Value' : silver_events.filter(col('conversion_value').isNull()).count(),
    'Null click_x sentinel (-1)' : silver_events.filter(col('click_x') == -1.0).count(),
    'Null click_y sentinel (-1)' : silver_events.filter(col('click_y') == -1.0).count()
}

all_passed = True
for label, count in silver_events_checks.items():
    pct = (count / total_rows * 100) if total_rows else 0
    status = '✅' if count == 0 else '⚠️'
    if count > 0 :
        all_passed = False
    print(f'{status} {label:<35} - {count:>8,} ({pct:5.2f}%)')

print('-' * 10)
print(f'Total rows in this batch : {total_rows:,}')
print(f'Overall Data Quality : {'✅ ALL CLEAN' if all_passed else '⚠️ SOME ISSUES'}')
print('*' * 10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Layer — Delta MERGE

# COMMAND ----------

# Transformed Batch to Silver via Delta MERGE semantics : 
# WHEN MATCHED -> UPDATE ALL COLUMNS
# WHEN NOT MATCHED -> INSERT ALL COLUMNS

if spark.catalog.tableExists(silver_table) :
    silver_delta = DeltaTable.forName(spark, silver_table)


    merge_result = (
        silver_delta.alias('silver')
        .merge(
            silver_events.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f'✅ MERGE Complete -> {silver_table}')
else :
    (
        silver_events.write
                    .format('delta')
                    .mode('append')
                    .option('mergeSchema', 'true')
                    .saveAsTable(silver_table)
    )
    print(f'✅ Silver table created -> {silver_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Written Validation

# COMMAND ----------

print('*' * 10)
print(f'Post - write Summary - {silver_table}')
print('*' * 10)

spark.sql(f'''
          SELECT 
            COUNT(*) AS total_rows,
            COUNT(DISTINCT event_id) AS unique_events,
            COUNT(DISTINCT user_id) AS unique_users,
            MIN(event_timestamp) AS earliest_event,
            MAX(event_timestamp) AS latest_event,
            ROUND(SUM(conversion_value), 2) AS totla_revenue,
            MAX(etl_processed_at) AS last_etl_run
          FROM {silver_table}
          ''').show(truncate=False)


spark.sql(f'''
          SELECT 
            event_type,
            COUNT(*) AS event_count,
            ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
            ROUND(SUM(conversion_value), 1) AS total_conversion_value
          FROM {silver_table}
          GROUP BY event_type
          ORDER BY event_count DESC
          ''').show(truncate=False)