# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema
# MAGIC

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_products'
silver_table = f'{silver_schema}.silver_products'

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

merge_key = 'product_id'

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

silver_products_df = spark.sql(f'''
                            SELECT *
                            FROM {bronze_table}
                            WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
                            ''')

batch_row_count = silver_products_df.count()
print(f'Bronze rows in this batch (pre_dedup): {batch_row_count}')                            

if batch_row_count == 0:
    print('No new data in bronze table')
    dbutils.notebook.exit('NO NEW DATA IN BRONZE TABLE')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy('product_id').orderBy(col('launch_date').desc_nulls_last())

silver_products_df = (
    silver_products_df
    .withColumn('rank', row_number().over(dedup_window))
    .filter(col('rank') == 1)
    .drop('rank')
)

print(f'After deduplication : {silver_products_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## price — Clean, Cast & Validate

# COMMAND ----------

# Strip Non-Numeric Characters from price Column

silver_products_df = silver_products_df.withColumn(
    'price',
    regexp_replace(trim(col('price')), r'[^0-9\.]', '')
)

# COMMAND ----------

# Cast price to Double

silver_products_df = silver_products_df.withColumn(
    'price',
    expr('try_cast(price as double)')
)

# COMMAND ----------

# Nullify Zero or Negative Prices

silver_products_df = silver_products_df.withColumn(
    "price", when(col("price") <= 0, lit(None)).otherwise(col("price"))
)

# COMMAND ----------

# Attempt Price Recovery from cost_price When Discount is Zero

silver_products_df = silver_products_df.withColumn(
    'price',
    when(
        col('price').isNull()
        & col('cost_price').isNotNull()
        & (col('discount_pct') == 0),
        col('cost_price')
    ).otherwise(col('price'))
)

still_price_nulls = silver_products_df.filter(col('price').isNull()).count()
print(f'After price recovery attempt — still null : {still_price_nulls:,}')


# COMMAND ----------

# MAGIC %md
# MAGIC ## rating — Range Validation

# COMMAND ----------

# Nullify Out-of-Range rating Values and Add Missing Flag

out_of_range_rating = silver_products_df.filter(
    col('rating').isNotNull() & ((col('rating') < 0) | (col('rating') > 5))
).count()
print(f'Out-of-range rating rows: {out_of_range_rating:,}')

silver_products_df = silver_products_df.withColumn(
    'rating',
    when((col('rating') < 0) | (col('rating') > 5), lit(None)).otherwise(col('rating'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flag Missing rating

# COMMAND ----------

silver_products_df = silver_products_df.withColumn(
    'rating_missing_flag',
    when(col('rating').isNull(), lit(True)).otherwise(lit(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## stock_quantity — Validate & Fill

# COMMAND ----------

# Coerce Negative or Null stock_quantity to 0 (Out of Stock)

silver_products_df = silver_products_df.withColumn(
    "stock_quantity",
    when(col("stock_quantity") < 0, lit(0))
    .otherwise(coalesce(col("stock_quantity"), lit(0)))
    .cast(IntegerType()),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## category — Normalisation

# COMMAND ----------

silver_products_df = silver_products_df.withColumn(
    "category", initcap(trim(col("category")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Standardisation - is_active

# COMMAND ----------

silver_products_df = silver_products_df.withColumn(
    "is_active",
    when(lower(trim(col("is_active"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("is_active"))).isin("no", "false", "0"), lit(False))
    .otherwise(lit(True).cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Standardisation - is_featured

# COMMAND ----------

silver_products_df = silver_products_df.withColumn(
    "is_featured",
    when(lower(trim(col("is_featured"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("is_featured"))).isin("no", "false", "0"), lit(False))
    .otherwise(lit(False).cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## warranty_period - Free-text -> Integer Months

# COMMAND ----------

# Parse warranty_period to warranty_months Integer
# years → months | lifetime → 99999 | no warranty → 0 | null → 0

silver_products_df = silver_products_df.withColumn(
    "warranty_months",
    when(lower(col("warranty_period")).contains("lifetime"), lit(99999))
    .when(lower(col("warranty_period")).contains("no warranty"), lit(0))
    .when(
        lower(col("warranty_period")).contains("year"),
        regexp_extract(col("warranty_period"), r"(\d+)", 1).cast(IntegerType()) * 12,
    )
    .when(
        lower(col("warranty_period")).contains("month"),
        regexp_extract(col("warranty_period"), r"(\d+)", 1).cast(IntegerType()),
    )
    .otherwise(lit(0).cast(IntegerType())),
).drop("warranty_period")

# COMMAND ----------

# MAGIC %md
# MAGIC ## tags - String -> Array

# COMMAND ----------

# Parse tags Pipe-Delimited String to Array

silver_products_df = silver_products_df.withColumn(
    "tags_array",
    when(
        col("tags").isNotNull() & (trim(col("tags")) != ""),
        split(lower(trim(col("tags"))), r"\|"),
    ).otherwise(array().cast("array<string>")),
).drop("tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SKU — Upper-case & Trim

# COMMAND ----------

silver_products_df = silver_products_df.withColumn('sku', upper(trim(col('sku'))))

total_rows     = silver_products_df.count()
total_sku_rows = silver_products_df.select('sku').distinct().count()
print(f'Total rows : {total_rows:,} | Unique SKUs : {total_sku_rows:,}')
if total_rows != total_sku_rows:
    print('⚠️  Duplicate SKU rows detected')
else:
    print('✅  All SKU rows are unique')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill Missing - supplier_id

# COMMAND ----------

silver_products_df = silver_products_df.withColumn(
    "supplier_id", coalesce(upper(trim(col("supplier_id"))), lit("UNKNOWN"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## product_name & brand — Trim

# COMMAND ----------

silver_products_df = (
    silver_products_df
    .withColumn('product_name', trim(col('product_name')))
    .withColumn('brand',        trim(col('brand')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_products_df = (
    silver_products_df
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source',       lit('bronze_products'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Write Data Quality Checks

# COMMAND ----------

print('-' * 60)
print('  Silver Products — Data Quality Report (this batch)')
print('-' * 60)
total = silver_products_df.count()

checks = {
    'Null product_id'           : silver_products_df.filter(col('product_id').isNull()).count(),
    'Null price'                : silver_products_df.filter(col('price').isNull()).count(),
    'Null cost_price'           : silver_products_df.filter(col('cost_price').isNull()).count(),
    'Null/invalid rating'       : silver_products_df.filter(col('rating_missing_flag') == True).count(),
    'Zero stock_quantity'       : silver_products_df.filter(col('stock_quantity') == 0).count(),
    'Unknown supplier_id'       : silver_products_df.filter(col('supplier_id') == 'UNKNOWN').count(),
    'Empty tags_array'          : silver_products_df.filter(size(col('tags_array')) == 0).count(),
    'Null launch_date'          : silver_products_df.filter(col('launch_date').isNull()).count(),
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

# Transformed Batch to Silver via Delta MERGE semantics : 
# WHEN MATCHED -> UPDATE ALL COLUMNS
# WHEN NOT MATCHED -> INSERT ALL COLUMNS

if spark.catalog.tableExists(silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)

    (
        silver_delta.alias('silver')
        .merge(
            silver_products_df.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅  MERGE complete → {silver_table}")

else:
    (
        silver_products_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(silver_table)
    )
    print(f"✅  Silver table created → {silver_table}")