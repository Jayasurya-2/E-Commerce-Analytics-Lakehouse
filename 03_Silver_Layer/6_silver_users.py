# Databricks notebook source
# MAGIC %md
# MAGIC ## Config Schema

# COMMAND ----------

silver_schema = 'e_commerce.silver'
bronze_table = 'e_commerce.bronze.bronze_users'
silver_table = f'{silver_schema}.silver_users'

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

Merge_Key = 'user_id'

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

silver_users_df = spark.sql(f'''
                            SELECT *
                            FROM {bronze_table}
                            WHERE _ingest_timestamp > CAST('{last_processed_ts}' AS TIMESTAMP)
                            ''')

batch_row_count = silver_users_df.count()
print(f'Bronze rows in this batch (pre_dedup): {batch_row_count}')                            

if batch_row_count == 0:
    print('No new data in bronze table')
    dbutils.notebook.exit('NO NEW DATA IN BRONZE TABLE')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy("user_id").orderBy(col("last_login").desc_nulls_last())

silver_users_df = (
    silver_users_df
    .withColumn("rank", row_number().over(dedup_window))
    .filter(col("rank") == 1)
    .drop("rank")
)

print(f'After deduplication : {silver_users_df.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Integrity — last_login Must Not Precede signup_date

# COMMAND ----------

# Nullify last_login That Precedes signup_date

bad_timeline = silver_users_df.filter(
    col("signup_date").isNotNull()
    & col("last_login").isNotNull()
    & (col("last_login") < col("signup_date"))
).count()
print(f"Rows where last_login < signup_date (will be nullified): {bad_timeline:,}")

silver_users_df = silver_users_df.withColumn(
    "last_login",
    when(
        col("last_login").isNotNull()
        & col("signup_date").isNotNull()
        & (col("last_login") < col("signup_date")),
        lit(None),
    ).otherwise(col("last_login")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Standardisation - is_active

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    "is_active",
    when(lower(trim(col("is_active"))).isin("yes", "true", "1"), lit(True))
    .when(lower(trim(col("is_active"))).isin("no", "false", "0"), lit(False))
    .otherwise(lit(False).cast(BooleanType())),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Standardisation - newsletter_subscribed

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    'newsletter_subscribed',
    when(lower(trim(col('newsletter_subscribed'))).isin('yes', 'true', '1'), lit(True))
     .when(lower(trim(col('newsletter_subscribed'))).isin('no',  'false', '0'), lit(False))
     .otherwise(lit(False).cast(BooleanType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## membership_tier — Casing Normalisation & Validation

# COMMAND ----------

valid_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']

silver_users_df = silver_users_df.withColumn(
    'membership_tier',
    when(
        initcap(trim(col('membership_tier'))).isin(valid_tiers),
        initcap(trim(col('membership_tier')))
    ).otherwise(lit('Unknown'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gender — Synonym Normalisation

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    "gender",
     when(lower(trim(col("gender"))).isin("male", "m"), lit("Male"))
    .when(lower(trim(col("gender"))).isin("female", "f"), lit("Female"))
    .when(lower(trim(col("gender"))).isin("non-binary", "nonbinary", "nb"), lit("Non-Binary"))
    .when(lower(trim(col("gender"))) == "other", lit("Other"))
    .otherwise(lit("Unknown")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## country — Sentinel Removal & ISO Code Expansion

# COMMAND ----------

country_code_map = {
    'us'      : 'United States',
    'usa'     : 'United States',
    'ca'      : 'Canada',
    'au'      : 'Australia',
    'uk'      : 'United Kingdom',
    'gb'      : 'United Kingdom',
    'uae'     : 'United Arab Emirates',
    'ae'      : 'United Arab Emirates',
    'sg'      : 'Singapore',
    'in'      : 'India',
}

code_map_expr = create_map(
    *[item for pair in [(lit(k), lit(v)) for k, v in country_code_map.items()] for item in pair]
)

silver_users_df = (
    silver_users_df
    .withColumn('_country_key', lower(trim(col('country'))))
    .withColumn(
        'country',
        when(col('_country_key') == 'unknown', lit(None))
        .when(code_map_expr[col('_country_key')].isNotNull(), code_map_expr[col('_country_key')])
        .otherwise(trim(col('country')))
    )
    .drop('_country_key')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## email — Lower-case & Format Validation

# COMMAND ----------

#Lowercase Email and Add email_valid_flag

email_regex = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'

silver_users_df = (
    silver_users_df
    .withColumn('email', lower(trim(col('email'))))
    .withColumn(
        'email_valid_flag',
        when(col('email').isNotNull() & col('email').rlike(email_regex), lit(True))
        .otherwise(lit(False))
    )
)

invalid_emails = silver_users_df.filter(col('email_valid_flag') == False).count()
print(f'Invalid / missing emails (flagged, not dropped): {invalid_emails:,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## age — Outlier Nullification

# COMMAND ----------

implausible_ages = silver_users_df.filter(
    col('age').isNotNull() & ((col('age') < 10) | (col('age') > 100))
).count()
print(f'Implausible age values nullified: {implausible_ages:,}')

silver_users_df = silver_users_df.withColumn(
    'age',
    when((col('age') < 10) | (col('age') > 100), lit(None))
    .otherwise(col('age').cast(IntegerType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## total_orders — Integer & Validate

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    'total_orders',
    when(col('total_orders') < 0, lit(None)).otherwise(col('total_orders').cast(IntegerType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## lifetime_value — Validate

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    'lifetime_value',
    when(col('lifetime_value') < 0, lit(None)).otherwise(col('lifetime_value'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## loyalty_points — Fill Nulls with 0

# COMMAND ----------

silver_users_df = silver_users_df.withColumn(
    'loyalty_points',
    coalesce(col('loyalty_points'), lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remaining String Columns — Trim, Normalise & Fill

# COMMAND ----------

silver_users_df = (
    silver_users_df
    .withColumn('name',  trim(col('name')))
    .withColumn('phone', trim(col('phone')))
    .withColumn('city',  initcap(trim(col('city'))))
    .withColumn('_occ_clean', initcap(trim(col('occupation'))))
    .withColumn('occupation',
                when(col('_occ_clean').isin('', 'None', 'N/A', 'Na'), lit(None))
                 .otherwise(col('_occ_clean')))
    .drop('_occ_clean')
    .withColumn('education_level', trim(col('education_level')))
    .withColumn('preferred_language', coalesce(initcap(trim(col('preferred_language'))), lit('Unknown')))
    .withColumn('timezone', coalesce(trim(col('timezone')),           lit('Unknown')))
    .withColumn('referral_source', coalesce(trim(col('referral_source')),    lit('Unknown')))
    .withColumn('preferred_category', coalesce(trim(col('preferred_category')), lit('Unknown')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Flags for Gold Layer Awareness

# COMMAND ----------

silver_users_df = (
    silver_users_df
    .withColumn('last_login_missing_flag',
                when(col('last_login').isNull(), lit(True)).otherwise(lit(False)))
    .withColumn('lifetime_value_missing_flag',
                when(col('lifetime_value').isNull(), lit(True)).otherwise(lit(False)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

silver_users_df = (
    silver_users_df
    .withColumn('etl_processed_at', current_timestamp())
    .withColumn('etl_source',       lit('bronze_delta'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Write Data Quality Checks

# COMMAND ----------

print('-' * 60)
print('  Silver Users — Data Quality Report (this batch)')
print('-' * 60)
total = silver_users_df.count()

checks = {
    'Null user_id'                : silver_users_df.filter(col('user_id').isNull()).count(),
    'Null signup_date'            : silver_users_df.filter(col('signup_date').isNull()).count(),
    'Missing last_login'          : silver_users_df.filter(col('last_login_missing_flag') == True).count(),
    'Invalid / null email'        : silver_users_df.filter(col('email_valid_flag') == False).count(),
    'Null age'                    : silver_users_df.filter(col('age').isNull()).count(),
    'Unknown gender'              : silver_users_df.filter(col('gender') == 'Unknown').count(),
    'Unknown membership_tier'     : silver_users_df.filter(col('membership_tier') == 'Unknown').count(),
    'Null country'                : silver_users_df.filter(col('country').isNull()).count(),
    'Null city'                   : silver_users_df.filter(col('city').isNull()).count(),
    'Null total_orders'           : silver_users_df.filter(col('total_orders').isNull()).count(),
    'Missing lifetime_value'      : silver_users_df.filter(col('lifetime_value_missing_flag') == True).count(),
    'Unknown preferred_language'  : silver_users_df.filter(col('preferred_language') == 'Unknown').count(),
    'Unknown timezone'            : silver_users_df.filter(col('timezone') == 'Unknown').count(),
    'Unknown referral_source'     : silver_users_df.filter(col('referral_source') == 'Unknown').count(),
    'Unknown preferred_category'  : silver_users_df.filter(col('preferred_category') == 'Unknown').count(),
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
            silver_users_df.alias('new'),
            f'silver.{merge_key} = new.{merge_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅  MERGE complete → {silver_table}")

else:
    (
        silver_users_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable(silver_table)
    )
    print(f"✅  Silver table created → {silver_table}")