# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema
# MAGIC

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_reviews'
silver_table = f'{silver_schema}.silver_reviews'

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

silver_reviews_df = spark.sql(f'''
                            SELECT *
                            FROM {bronze_table}
                            WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
                            ''')

batch_row_count = silver_reviews_df.count()
print(f'Bronze rows in this batch (pre_dedup): {batch_row_count}')                            

if batch_row_count == 0:
    print('No new data in bronze table')
    dbutils.notebook.exit('NO NEW DATA IN BRONZE TABLE')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spam Removal — Filter Before Any Other Processing
# MAGIC MAGIC Spam rows must never enter Silver. Null `flagged_as_spam` defaults to `'No'` (assume genuine).

# COMMAND ----------

# Normalise and Drop Spam Records

silver_reviews_df = silver_reviews_df.withColumn(
    "flagged_as_spam", coalesce(trim(upper(col("flagged_as_spam"))), lit("NO"))
)

spam_count = silver_reviews_df.filter(col('flagged_as_spam') == 'YES').count()
print(f'Spam records to drop : {spam_count:,}')

silver_reviews_df = silver_reviews_df.filter(col("flagged_as_spam") != 'YES').drop('flagged_as_spam')
print(f'After spam removal : {silver_reviews_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy('user_id', 'product_id').orderBy(col('review_date').desc_nulls_last())
print(f'Before deduplication : {silver_reviews_df.count():,}')

silver_reviews_df = (
    silver_reviews_df
    .withColumn('rank', row_number().over(dedup_window))
    .filter(col('rank') == 1)
    .drop('rank')
)

print(f'After deduplication : {silver_reviews_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## reply_date Must Not Precede review_date

# COMMAND ----------

bad_reply_dates = silver_reviews_df.filter(
    col('reply_date').isNotNull() & (col('reply_date') < col('review_date'))
).count()
print(f'Total bad reply dates (reply before review) : {bad_reply_dates:,}')

silver_reviews_df = silver_reviews_df.withColumn(
    'reply_date',
    when(
        col('reply_date').isNotNull() & (col('reply_date') < col('review_date')),
        lit(None)
    ).otherwise(col('reply_date'))
)

print(f'After fixing bad reply dates : {silver_reviews_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## rating — Range Validation & Outlier Nullification

# COMMAND ----------

out_of_range_ratings = silver_reviews_df.filter(
    (col("rating") < 1.0) | (col("rating") > 5.0)
).count()
print(f"Out of range ratings : {out_of_range_ratings:,}")

silver_reviews_df = silver_reviews_df.withColumn(
    "rating",
    when((col("rating") < 1.0) | (col("rating") > 5.0), lit(None)).otherwise(col("rating")),
)

print(f'After fixing bad ratings : {silver_reviews_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## sentiment — Normalise & Derive from rating When Null

# COMMAND ----------

valid_sentiments = ["Positive", "Negative", "Neutral", "Mixed"]

silver_reviews_df = (
    silver_reviews_df
    .withColumn("_sent_std", initcap(trim(col("sentiment"))))
    .withColumn(
        "sentiment",
        when(col("_sent_std").isin(valid_sentiments), col("_sent_std")).otherwise(lit(None)),
    )
    .drop("_sent_std")
)

# COMMAND ----------

# Derive sentiment from rating where still null

silver_reviews_df = silver_reviews_df.withColumn(
    "sentiment",
    when(
        col("sentiment").isNull() & col("rating").isNotNull(),
        when(col("rating") >= 4.0, lit("Positive"))
        .when(col("rating") == 3.0, lit("Neutral"))
        .when(col("rating") <= 2.0, lit("Negative")),
    ).otherwise(col("sentiment")),
)

# COMMAND ----------

# Both null → Unknown
silver_reviews_df = silver_reviews_df.withColumn(
    "sentiment", coalesce(col("sentiment"), lit("Unknown"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## verified_purchase - Boolean Standardisation

# COMMAND ----------

silver_reviews_df = silver_reviews_df.withColumn(
    "verified_purchase",
    when(lower(trim(col("verified_purchase"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("verified_purchase"))).isin("no", "false", "0"), lit(False))
    .otherwise(col("verified_purchase").cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## seller_replied - Boolean Standardisation

# COMMAND ----------

silver_reviews_df = silver_reviews_df.withColumn(
    "seller_replied",
    when(lower(trim(col("seller_replied"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("seller_replied"))).isin("no", "false", "0"), lit(False))
    .otherwise(col("seller_replied").cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## seller_replied vs reply_date — Consistency Check

# COMMAND ----------

# Reconcile seller_replied and reply_date for Consistency
# reply_date present → seller definitely replied

silver_reviews_df = silver_reviews_df.withColumn(
    "seller_replied",
    when(col("reply_date").isNotNull(), lit(True)).otherwise(col("seller_replied")),
)

# seller_replied=False → no reply_date should exist
silver_reviews_df = silver_reviews_df.withColumn(
    "reply_date",
    when(col("seller_replied") == False, lit(None)).otherwise(col("reply_date"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vote Counts & media_count — Fill Nulls with 0

# COMMAND ----------

# Fill Null Vote Counts and media_count with 0

silver_reviews_df = (
    silver_reviews_df
    .withColumn('helpful_votes', coalesce(col('helpful_votes'),   lit(0.0)))
    .withColumn('unhelpful_votes', coalesce(col('unhelpful_votes'), lit(0.0)))
    .withColumn('media_count', coalesce(col('media_count'),     lit(0.0)).cast(IntegerType()))
)

# COMMAND ----------

# Negative votes are impossible — floor at 0

silver_reviews_df = silver_reviews_df.withColumn(
    "helpful_votes",   when(col("helpful_votes") < 0,   lit(0.0)).otherwise(col("helpful_votes"))
).withColumn(
    "unhelpful_votes", when(col("unhelpful_votes") < 0, lit(0.0)).otherwise(col("unhelpful_votes"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text Fields — Trim & Fill

# COMMAND ----------

# Fill Missing Review Title and Text

silver_reviews_df = silver_reviews_df.withColumn(
    "review_title", coalesce(trim(col("review_title")), lit("No Review Title"))
).withColumn(
    "review_text", coalesce(trim(col("review_text")), lit("No Review Text"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Categorical String Columns — Trim & Fill

# COMMAND ----------

silver_reviews_df = (
    silver_reviews_df
    .withColumn('product_name', trim(col('product_name')))
    .withColumn('category', trim(col('category')))
    .withColumn('review_source', coalesce(trim(col('review_source')),   lit('Unknown')))
    .withColumn('review_language', coalesce(trim(col('review_language')), lit('Unknown')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived Columns for Gold Layer Readiness
# MAGIC

# COMMAND ----------

# Add Derived Columns — total_votes, has_media, reply_lag_days, etc.

silver_reviews_df = (
    silver_reviews_df
    .withColumn('total_votes', col('helpful_votes') + col('unhelpful_votes'))
    .withColumn('has_media', col('media_count') > 0)
    .withColumn('reply_lag_days',
                when(
                    col('reply_date').isNotNull() & col('review_date').isNotNull(),
                    date_diff(col('reply_date'), col('review_date'))
                ).otherwise(lit(None)))
    .withColumn('review_text_length',   length(col('review_text')))
    .withColumn('rating_missing_flag',  col('rating').isNull())
)

# COMMAND ----------

# Flag seller_replied=True But Missing reply_date

silver_reviews_df = silver_reviews_df.withColumn(
    'reply_date_missing_flag',
    when(
        (col('seller_replied') == True) & col('reply_date').isNull(),
        lit(True)
    ).otherwise(lit(False))
)

print(f'Replied but no date flagged : {silver_reviews_df.filter(col("reply_date_missing_flag") == True).count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_reviews_df = (
    silver_reviews_df
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source',       lit('bronze_delta'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Write Data Quality Checks

# COMMAND ----------

print('-' * 60)
print('  Silver Reviews — Data Quality Report (this batch)')
print('-' * 60)
total = silver_reviews_df.count()

checks = {
    'Null review_id'              : silver_reviews_df.filter(col('review_id').isNull()).count(),
    'Null user_id'                : silver_reviews_df.filter(col('user_id').isNull()).count(),
    'Null product_id'             : silver_reviews_df.filter(col('product_id').isNull()).count(),
    'Null review_date'            : silver_reviews_df.filter(col('review_date').isNull()).count(),
    'Null / invalid rating'       : silver_reviews_df.filter(col('rating_missing_flag') == True).count(),
    'Unknown sentiment'           : silver_reviews_df.filter(col('sentiment') == 'Unknown').count(),
    'Unknown review_source'       : silver_reviews_df.filter(col('review_source') == 'Unknown').count(),
    'Unknown review_language'     : silver_reviews_df.filter(col('review_language') == 'Unknown').count(),
    'Replied but no reply_date'   : silver_reviews_df.filter(col('reply_date_missing_flag') == True).count(),
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
            silver_reviews_df.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅  MERGE complete → {silver_table}")

else:
    (
        silver_reviews_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(silver_table)
    )
    print(f"✅  Silver table created → {silver_table}")