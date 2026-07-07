# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema
# MAGIC

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_order_items'
silver_table = f'{silver_schema}.silver_order_items'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import * 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Key

# COMMAND ----------

merge_key = 'order_item_id'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read last processed TimeStamp from Silver
# MAGIC WaterMark Stratergy

# COMMAND ----------

EPOCH_WATERMARK = "1900-01-01 00:00:00"

if spark.catalog.tableExists(silver_table):
    last_processed_ts = spark.sql(
        f"SELECT COALESCE(MAX(_ingest_timestamp), CAST('{EPOCH_WATERMARK}' AS TIMESTAMP)) AS wm FROM {silver_table}"
    ).collect()[0]["wm"]
else:
    last_processed_ts = EPOCH_WATERMARK

print(f"Silver watermark → last _ingest_timestamp processed : {last_processed_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Incremental Batch from Bronze

# COMMAND ----------

# Pull New / Updated Rows from Bronze Delta Table
# We push the watermark filter into bronze delta table so that we can avoid reading all the data via 
# _ingest_timestamp
# This avoids a FULL TABLE SCAN ON EVERY PIPELINE RUN

silver_order_items_df = spark.sql(f"""
    SELECT * FROM {bronze_table}
    WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
""")

batch_row_count = silver_order_items_df.count()
print(f"Bronze row count: {batch_row_count:,}")
silver_order_items_df.printSchema()

if batch_row_count == 0:
    print("ℹ️   No new Bronze data since last run. Silver is already current. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy('order_item_id').orderBy(col('order_item_id').desc())

silver_order_items_df = (
    silver_order_items_df
    .withColumn('rank', row_number().over(dedup_window))
    .filter(col('rank') == 1)
    .drop('rank')
)

print(f'Duplicates Removed : {(batch_row_count - silver_order_items_df.count()):,}')
print(f'Batch rows after DeDuplication : {silver_order_items_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Column — discount_amount
# MAGIC

# COMMAND ----------

# Update Discount Amounts in Silver Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    "discount_amount", coalesce(col("discount_amount"), lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Column — unit_price

# COMMAND ----------

# Calculate Unit Price for Silver Order Items DataFrame
# unit_price → (subtotal + discount) / quantity gives the original unit price

silver_order_items_df = silver_order_items_df.withColumn(
    'unit_price',
    when(
        col('unit_price').isNull() & (col('quantity') != 0),
        (coalesce(col('subtotal'), lit(0)) + coalesce(col('discount_amount'), lit(0)))
        / col('quantity')
    ).otherwise(col('unit_price'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Column — subtotal

# COMMAND ----------

# Compute Missing Subtotal in Silver Order Items DataFrame
# subtotal → quantity * unit_price  (gross, before discount)

silver_order_items_df = silver_order_items_df.withColumn(
    "subtotal",
    when(
        col("subtotal").isNull()
        & col("quantity").isNotNull()
        & col("unit_price").isNotNull(),
        col("quantity") * col("unit_price"),
    ).otherwise(col("subtotal")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Column — tax_rate

# COMMAND ----------

# Calculate Tax Rate for Silver Order Items DataFrame
# tax_rate → tax_amount / subtotal

silver_order_items_df = silver_order_items_df.withColumn(
    "tax_rate",
    when(
        col("tax_rate").isNull()
        & col("subtotal").isNotNull()
        & (col("subtotal") != 0)
        & col("tax_amount").isNotNull(),
        col("tax_amount") / col("subtotal"),
    ).otherwise(col("tax_rate")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Column — tax_amount

# COMMAND ----------

# Fill Missing Tax Amounts Using Subtotal and Tax Rate
# tax_amount → subtotal * tax_rate

silver_order_items_df = silver_order_items_df.withColumn(
    "tax_amount",
    when(
        col("tax_amount").isNull()
        & col("subtotal").isNotNull()
        & col("tax_rate").isNotNull(),
        col("subtotal") * col("tax_rate"),
    ).otherwise(col("tax_amount")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Unrecoverable Tax Rows & Round

# COMMAND ----------

# Filter Out Null Tax Values & Round Financial Columns

before = silver_order_items_df.count()
print(f'Rows before drop : {before:,}')

silver_order_items_df = silver_order_items_df.filter(
    ~(col("tax_rate").isNull() & col("tax_amount").isNull())
)

dropped = before - silver_order_items_df.count()
print(f"Rows dropped (unresolvable tax) : {dropped:,}")

silver_order_items_df = (
    silver_order_items_df
    .withColumn('tax_amount', bround(col('tax_amount'), 2))
    .withColumn('tax_rate',   bround(col('tax_rate'),   4))
)

print(f'Total rows after clean : {silver_order_items_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returned Flag & Return Reason

# COMMAND ----------

silver_order_items_df = silver_order_items_df.withColumn('returned', initcap(trim(col('returned'))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Return Status in Silver Order Items DataFrame

# COMMAND ----------

silver_order_items_df = silver_order_items_df.withColumn(
    'returned',
    when(col('returned').isNull(), lit('No')).otherwise(col('returned'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Return Reason for Silver Order Items DataFrame

# COMMAND ----------

silver_order_items_df = silver_order_items_df.withColumn(
    'return_reason',
    when(col('returned') == 'No', lit('N/A'))
     .when((col('returned') == 'Yes') & col('return_reason').isNull(), lit('Unknown'))
     .otherwise(col('return_reason'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fulfillment Type

# COMMAND ----------

# Fill Missing Fulfillment Type in Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    'fulfillment_type',
    trim(initcap(coalesce(col('fulfillment_type'), lit('Standard'))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize - is_digital

# COMMAND ----------

# Standardize is_digital Column in Silver Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    'is_digital',
    initcap(trim(coalesce(col('is_digital'), lit('No'))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format - gift_wrap

# COMMAND ----------

# Format Gift Wrap Column in Silver Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    'gift_wrap',
    initcap(trim(coalesce(col('gift_wrap'), lit('No'))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling NUll - seller_id

# COMMAND ----------

# Replace Null Seller IDs with UNKNOWN

silver_order_items_df = silver_order_items_df.withColumn(
    "seller_id",
    when(col("seller_id").isNull(), lit("UNKNOWN")).otherwise(col("seller_id")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Null - warehouse_id

# COMMAND ----------

# Handle Null Warehouse IDs in Silver Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    "warehouse_id",
    when(col("warehouse_id").isNull(), lit("UNKNOWN")).otherwise(col("warehouse_id")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ship_date Missing Flag

# COMMAND ----------

# Identify Missing Ship Dates in Silver Order Items DataFrame

silver_order_items_df = silver_order_items_df.withColumn(
    "ship_date_missing_flag",
    when(col("ship_date").isNull(), lit(1)).otherwise(lit(0))
)

print(f'Total ship_date flagged rows : {silver_order_items_df.filter(col("ship_date_missing_flag") == 1).count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_order_items_df = (
    silver_order_items_df
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source',       lit('bronze_delta'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Pre - Write Data Quaity Checks

# COMMAND ----------

print('*' * 30)
print('  Silver Order Items — Data Quality Report (this batch)')
print('*' * 30)
total = silver_order_items_df.count()

checks = {
    'Null order_item_id'      : silver_order_items_df.filter(col('order_item_id').isNull()).count(),
    'Null order_id'           : silver_order_items_df.filter(col('order_id').isNull()).count(),
    'Null product_id'         : silver_order_items_df.filter(col('product_id').isNull()).count(),
    'Zero/negative quantity'  : silver_order_items_df.filter(col('quantity') <= 0).count(),
    'Negative unit_price'     : silver_order_items_df.filter(col('unit_price') < 0).count(),
    'Null tax_amount'         : silver_order_items_df.filter(col('tax_amount').isNull()).count(),
    'Missing ship_date'       : silver_order_items_df.filter(col('ship_date_missing_flag') == 1).count(),
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
print('-' * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Layer — Delta MERGE

# COMMAND ----------

# Merge New Batch into Silver Delta Table
# MERGE on order_item_id:
#   WHEN MATCHED     → UPDATE ALL  (late-arriving corrections overwrite stale rows)
#   WHEN NOT MATCHED → INSERT ALL  (net-new items)
# Running this notebook multiple times produces the same Silver state — fully idempotent.

if spark.catalog.tableExists(silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)

    (
        silver_delta.alias('silver')
        .merge(
            silver_order_items_df.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅  MERGE complete → {silver_table}")

else:
    (
        silver_order_items_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(silver_table)
    )
    print(f"✅  Silver table created → {silver_table}")