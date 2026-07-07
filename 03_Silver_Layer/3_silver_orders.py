# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema
# MAGIC

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_orders'
silver_table = f'{silver_schema}.silver_orders'

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

merge_key = 'order_id'

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

silver_orders_df = spark.sql(f'''
                            SELECT *
                            FROM {bronze_table}
                            WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
                            ''')

batch_row_count = silver_orders_df.count()
print(f'Bronze rows in this batch (pre_dedup): {batch_row_count}')                            

if batch_row_count == 0:
    print('No new data in bronze table')
    dbutils.notebook.exit('NO NEW DATA IN BRONZE TABLE')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy('order_id').orderBy(col("order_date").desc())

silver_orders_df = (
    silver_orders_df.withColumn("rank", row_number().over(dedup_window))
    .filter(col("rank") == 1)
    .drop("rank")
)

print(f'Duplicates Removed : {(batch_row_count - silver_orders_df.count()):,}')
print(f'Batch rows after DeDuplication : {silver_orders_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Integrity Checks

# COMMAND ----------

# Nullify actual_delivery Before order_date
# actual_delivery before order_date is physically impossible

bad_actual = silver_orders_df.filter(
    col('actual_delivery').isNotNull() &
    (col('actual_delivery') < to_date(col('order_date')))
).count()
print(f'actual_delivery before order_date (will be nullified): {bad_actual:,}')

silver_orders_df = silver_orders_df.withColumn(
    'actual_delivery',
    when(
        col('actual_delivery').isNotNull() &
        (col('actual_delivery') < to_date(col('order_date'))),
        lit(None)
    ).otherwise(col('actual_delivery'))
)

# COMMAND ----------

# Nullify estimated_delivery Before order_date

silver_orders_df = silver_orders_df.withColumn(
    'estimated_delivery',
    when(
        col('estimated_delivery').isNotNull() &
        (col('estimated_delivery') < to_date(col('order_date'))),
        lit(None)
    ).otherwise(col('estimated_delivery'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Standardisation — is_gift

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "is_gift",
    when(lower(trim(col("is_gift"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("is_gift"))).isin("no", "false", "0"), lit(False))
    .otherwise(lit(False).cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## payment_method — Normalise Casing

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "payment_method", initcap(trim(col("payment_method")))
)

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "payment_method", coalesce(col("payment_method"), lit("Unknown"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## channel — Synonym Normalisation

# COMMAND ----------

# Normalise channel Values to Standard Labels

channel_map = {
    "mobile"      : "Mobile Web",
    "desktop"     : "Desktop Web",
    "web"         : "Desktop Web",
    "ios app"     : "iOS App",
    "android app" : "Android App",
    "tv app"      : "TV App",
    "partner site": "Partner Site",
    "tablet"      : "Tablet Web",
}

channel_map_expr = create_map(
    *[item for pair in [(lit(k), lit(v)) for k, v in channel_map.items()] for item in pair]
)

silver_orders_df = (
    silver_orders_df
    .withColumn("_ch_lower", lower(trim(col("channel"))))
    .withColumn("channel", coalesce(channel_map_expr[col("_ch_lower")], lit("Unknown")))
    .drop("_ch_lower")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## shipping_type — Normalise Casing

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "shipping_type", coalesce(initcap(trim(col('shipping_type'))), lit('Unknown'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## coupon_code — Standardise

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "coupon_code",
    when(col("coupon_code").isNotNull(), upper(trim(col("coupon_code")))).otherwise(lit(None)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived Flag — has_coupon

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn('has_coupon', col('coupon_code').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## discount_amount

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "discount_amount",
    when(col("discount_amount").isNull() & (~col("has_coupon")), lit(0.0))
    .otherwise(col("discount_amount")),
)

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "discount_amount",
    when(col("discount_amount") < 0, lit(None)).otherwise(col("discount_amount")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## shipping_fee — Median Imputation by shipping_type

# COMMAND ----------

shipping_medians = (
    silver_orders_df
    .filter(col('shipping_fee').isNotNull())
    .groupBy('shipping_type')
    .agg(percentile_approx('shipping_fee', 0.5).alias('median_shipping_fee'))
)

global_shipping_median = (
    silver_orders_df
    .filter(col('shipping_fee').isNotNull())
    .agg(percentile_approx('shipping_fee', 0.5).alias('median_val'))
    .collect()[0]['median_val']
)

silver_orders_df = (
    silver_orders_df
    .join(shipping_medians, on='shipping_type', how='left')
    .withColumn(
        'shipping_fee',
        coalesce(col('shipping_fee'), col('median_shipping_fee'), lit(global_shipping_median))
    )
    .drop('median_shipping_fee')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## tax_amount — Fill Where Inferable

# COMMAND ----------

# Resolve tax_amount — Cancelled/Failed Orders → 0, Negative → Null
# Cancelled / failed orders legitimately have 0 tax

silver_orders_df = silver_orders_df.withColumn(
    'tax_amount',
    when(col('tax_amount').isNull() & col('status').isin('cancelled', 'failed'), lit(0.0))
    .otherwise(col('tax_amount'))
)

# Negative tax is impossible — nullify
silver_orders_df = silver_orders_df.withColumn(
    'tax_amount',
    when(col('tax_amount') < 0, lit(None)).otherwise(col('tax_amount'))
)

# COMMAND ----------

# Flag unresolved tax for downstream awareness

silver_orders_df = silver_orders_df.withColumn('tax_missing_flag', col('tax_amount').isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## total_amount — Validate & Back-Calculate

# COMMAND ----------

# Back-Calculate total_amount When Null and Components Available

silver_orders_df = silver_orders_df.withColumn(
    'total_amount',
    when(
        col('total_amount').isNull()
        & col('tax_amount').isNotNull()
        & col('shipping_fee').isNotNull()
        & col('discount_amount').isNotNull(),
        round(
            coalesce(col('tax_amount'), lit(0.0))
            + coalesce(col('shipping_fee'), lit(0.0))
            - coalesce(col('discount_amount'), lit(0.0)),
            2
        )
    ).otherwise(col('total_amount'))
)

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    'total_amount',
    when(col('total_amount') < 0, lit(None)).otherwise(col('total_amount'))
)

remaining_null_total = silver_orders_df.filter(col('total_amount').isNull()).count()
print(f'total_amount still null after back-calculation: {remaining_null_total:,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## num_items — Cast Float → Integer

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "num_items",
    when(col("num_items") <= 0, lit(None)).otherwise(col("num_items").cast(IntegerType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_rating — Validate Range

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    'order_rating',
    when(
        col('order_rating').isNotNull() & ((col('order_rating') < 1) | (col('order_rating') > 5)),
        lit(None)
    ).otherwise(col('order_rating'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flag - valid rating

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn('has_rating', col('order_rating').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ip_address — PII Masking

# COMMAND ----------

# Mask Last Octet of ip_address for PII Compliance

silver_orders_df = (
    silver_orders_df
    .withColumn(
        'ip_address_masked',
        when(
            col('ip_address').isNotNull(),
            regexp_replace(col('ip_address'), r'\.\d+$', '.xxx')
        ).otherwise(lit(None))
    )
    .drop('ip_address')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill Missing — warehouse_id & currency

# COMMAND ----------

silver_orders_df = (
    silver_orders_df
    .withColumn('warehouse_id', coalesce(trim(col('warehouse_id')), lit('UNKNOWN')))
    .withColumn('currency',     coalesce(upper(trim(col('currency'))), lit('UNKNOWN')))
)

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    'warehouse_city',
    when(
        col('warehouse_id') != 'UNKNOWN',
        regexp_extract(col('warehouse_id'), r'^([A-Za-z]+)', 1)
    ).otherwise(lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## status — Trim & Validate

# COMMAND ----------

valid_status = [
    'partial', 'pending', 'processing', 'failed', 'on_hold',
    'shipped', 'completed', 'returned', 'cancelled', 'refunded'
]

silver_orders_df = silver_orders_df.withColumn(
    'status',
    when(lower(trim(col('status'))).isin(valid_status), initcap(trim(col('status'))))
    .otherwise(lit('unknown'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delivery Missing Flag

# COMMAND ----------

silver_orders_df = silver_orders_df.withColumn(
    "actual_delivery_missing_flag", col("actual_delivery").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_orders_df = (
    silver_orders_df
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source',       lit('bronze_orders'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Pre - Write Data Quaity Checks

# COMMAND ----------

print('-' * 60)
print('  Silver Orders — Data Quality Report (this batch)')
print('-' * 60)
total = silver_orders_df.count()

checks = {
    'Null order_id'             : silver_orders_df.filter(col('order_id').isNull()).count(),
    'Null user_id'              : silver_orders_df.filter(col('user_id').isNull()).count(),
    'Null order_date'           : silver_orders_df.filter(col('order_date').isNull()).count(),
    'Null total_amount'         : silver_orders_df.filter(col('total_amount').isNull()).count(),
    'Null discount_amount'      : silver_orders_df.filter(col('discount_amount').isNull()).count(),
    'Null tax_amount'           : silver_orders_df.filter(col('tax_missing_flag') == True).count(),
    'Null num_items'            : silver_orders_df.filter(col('num_items').isNull()).count(),
    'Missing actual_delivery'   : silver_orders_df.filter(col('actual_delivery_missing_flag') == True).count(),
    'Unknown status'            : silver_orders_df.filter(col('status') == 'unknown').count(),
    'Unknown warehouse_id'      : silver_orders_df.filter(col('warehouse_id') == 'UNKNOWN').count(),
    'Unknown currency'          : silver_orders_df.filter(col('currency') == 'UNKNOWN').count(),
    'Unknown channel'           : silver_orders_df.filter(col('channel') == 'Unknown').count(),
    'No rating (expected)'      : silver_orders_df.filter(~col('has_rating')).count(),
}

all_passed = True
for label, count in checks.items():
    pct    = count / total * 100 if total else 0
    status = '✅' if count == 0 else '⚠️ '
    if count > 0: all_passed = False
    print(f'  {status}  {label:<35} {count:>8,}  ({pct:5.2f}%)')

print('-' * 60)
print(f"  Total rows in this batch : {total:,}")
print(f"  Overall DQ status        : {'✅ ALL CLEAN' if all_passed else '⚠️  REVIEW WARNINGS ABOVE'}")
print('=' * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Layer — Delta MERGE

# COMMAND ----------

# Transformed Batch to Silver via Delta MERGE semantics : 
# WHEN MATCHED -> UPDATE ALL COLUMNS
# WHEN NOT MATCHED -> INSERT ALL COLUMNS

if spark.catalog.tableExists(silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)

    (
        silver_delta.alias('silver')
        .merge(
            silver_orders_df.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅  MERGE complete → {silver_table}")

else:
    (
        silver_orders_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(silver_table)
    )
    print(f"✅  Silver table created → {silver_table}")