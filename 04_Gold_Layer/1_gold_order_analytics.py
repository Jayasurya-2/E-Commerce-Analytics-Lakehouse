# Databricks notebook source
# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

# Source Silver tables

silver_orders = 'e_commerce.silver.silver_orders'
silver_order_items = 'e_commerce.silver.silver_order_items'
silver_users = 'e_commerce.silver.silver_users'
silver_products = 'e_commerce.silver.silver_products'
silver_reviews = 'e_commerce.silver.silver_reviews'
silver_events = 'e_commerce.silver.silver_events'

# COMMAND ----------

# Target Gold table

gold_schema = 'e_commerce.gold'
gold_table = f'{gold_schema}.gold_order_analytics'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import uuid
from pyspark.sql.functions import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge key: one row per order in Gold

# COMMAND ----------

MERGE_KEY = 'order_id'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark fallback for very first run

# COMMAND ----------

EPOCH_WM = '1900-01-01 00:00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unique ID stamped on every row written in this run

# COMMAND ----------

ETL_BATCH_ID       = str(uuid.uuid4())

print(f"Gold table   : {gold_table}")
print(f"ETL batch ID : {ETL_BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Watermark

# COMMAND ----------

# We track two watermarks from the Gold table itself:
 
# 1. `max_order_date` — the latest `order_date` already written to Gold.
#    Catches **brand-new orders** that arrived after the last run.

# 2. `max_silver_etl_ts` — the latest `silver_etl_processed_at` already
#    written to Gold. Catches **corrections** to old orders that were
#    re-processed in Silver after the last Gold run.

# Both watermarks are needed. Without watermark 2, a corrected
# order from last month would never be re-merged into Gold.

if spark.catalog.tableExists(gold_table):
    wm = spark.sql(f"""
        SELECT
            COALESCE(MAX(order_date), CAST('{EPOCH_WM}' AS TIMESTAMP)) AS max_order_date,
            COALESCE(MAX(silver_etl_processed_at), CAST('{EPOCH_WM}' AS TIMESTAMP)) AS max_silver_etl_ts
        FROM {gold_table}
    """).collect()[0]

    max_order_date    = wm['max_order_date']
    max_silver_etl_ts = wm['max_silver_etl_ts']
else:
    # First run — pull everything
    max_order_date    = EPOCH_WM
    max_silver_etl_ts = EPOCH_WM

print(f"Watermark 1 — max order_date in Gold : {max_order_date}")
print(f"Watermark 2 — max silver_etl_processed_at in Gold : {max_silver_etl_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Orders (Incremental — Fact)
# MAGIC - silver_orders` is the **spine** of this Gold table.
# MAGIC - MAGIC Every join hangs off the orders we pull here.
# MAGIC - MAGIC We pull an order if it is new (order_date > watermark) OR
# MAGIC - MAGIC if Silver corrected it after our last run (etl_processed_at > watermark)

# COMMAND ----------

# Read Incremental Batch from Silver Orders

orders_df = spark.sql(f"""
    SELECT
        order_id,
        user_id,
        order_date,
        status,
        total_amount,
        discount_amount,
        shipping_fee,
        tax_amount,
        num_items,
        payment_method,
        channel,
        shipping_type,
        coupon_code,
        has_coupon,
        order_rating,
        actual_delivery,
        estimated_delivery,
        currency,
        warehouse_id,
        etl_processed_at    AS silver_etl_processed_at
    FROM {silver_orders}
    WHERE order_date       > CAST('{max_order_date}'    AS TIMESTAMP)
       OR etl_processed_at > CAST('{max_silver_etl_ts}' AS TIMESTAMP)
""")

batch_count = orders_df.count()
print(f"Orders in this incremental batch : {batch_count:,}")

# Early exit — nothing to do
if batch_count == 0:
    print("ℹ️  No new or updated orders since last run. Gold is already current.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Aggregate Child Tables
# MAGIC - **Critical rule — aggregate BEFORE joining.**
# MAGIC - If we join order_items (many rows per order) directly to orders (1 row per order)
# MAGIC - without aggregating first, every order gets duplicated once per item.
# MAGIC - That would make revenue calculations completely wrong.
# MAGIC - The same rule applies to reviews and events — many rows per user, so
# MAGIC - we aggregate to user level before joining.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items → Aggregate to Order Level

# COMMAND ----------

# Aggregate silver_order_items to One Row Per order_id
# We only aggregate items for the orders in our current batch.
# collect the batch order_ids first, then filter the items table.
# This avoids a full scan of the items table on every run.

batch_order_ids = orders_df.select('order_id').distinct()

items_agg = (
    spark.table(silver_order_items)

    # Only process items that belong to orders in this batch
    .join(batch_order_ids, on='order_id', how='inner')

    .groupBy('order_id')
    .agg(
        # How many line items are in this order?
        count('order_item_id').alias('total_line_items'),

        # Total physical units across all line items
        sum('quantity').alias('total_units_sold'),

        # Gross sales value before discount (subtotal = qty × unit_price)
        round(sum('subtotal'), 2).alias('items_gross_subtotal'),

        # Total discount applied at item level
        round(sum('discount_amount'), 2).alias('items_total_discount'),

        # Tax collected at item level
        round(sum('tax_amount'), 2).alias('items_total_tax'),

        # How many units were returned?
        sum(
            when(col('returned') == 'Yes', col('quantity'))
            .otherwise(0)
        ).alias('total_units_returned'),

        # How many distinct items were returned?
        count(
            when(col('returned') == 'Yes', 1)
        ).alias('return_line_count'),
    )
)

print(f"items_agg rows : {items_agg.count():,}  (should equal batch order count or less)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products → Aggregate to Order Level
# MAGIC - A single order may contain multiple different products.
# MAGIC - We join products → order_items to get the product attributes per order,
# MAGIC - then aggregate so each order gets one summary row.

# COMMAND ----------

# Join Products to Order Items, Then Aggregate to Order Level
# Step 1: join silver_products onto silver_order_items (item → product mapping)
items_with_products = (
    spark.table(silver_order_items)
    .join(batch_order_ids, on='order_id', how='inner')

    # Bring in product attributes via product_id
    .join(
        spark.table(silver_products).select(
            'product_id', 'product_name', 'category', 'brand',
            'price', 'cost_price', 'rating', 'stock_quantity'
        ),
        on='product_id',
        how='left'
    )
)

# COMMAND ----------

# Step 2: aggregate to order level — one row per order_id

products_agg = (
    items_with_products
    .groupBy('order_id')
    .agg(
        # How many distinct products are in this order?
        countDistinct('product_id').alias('distinct_products_ordered'),

        # Average product catalogue price across items in this order
        round(avg('price'), 2).alias('avg_product_price'),

        # Average product cost price — proxy for margin analysis
        round(avg('cost_price'), 2).alias('avg_product_cost_price'),

        # Average product review rating across items in this order
        round(avg('rating'), 2).alias('avg_product_rating'),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews → Aggregate to User Level
# MAGIC - Reviews are at user × product grain.
# MAGIC - We aggregate to user level (total reviews, average rating, sentiment)
# MAGIC - so one user row can be joined to each of their orders cleanly.

# COMMAND ----------

# Aggregate silver_reviews to One Row Per user_id
# Get the unique user_ids in this batch to scope the review read

batch_user_ids = orders_df.select('user_id').distinct()

reviews_agg = (
    spark.table(silver_reviews)
    .join(batch_user_ids, on='user_id', how='inner')

    .groupBy('user_id')
    .agg(
        # Total number of reviews this user has written
        count('review_id').alias('user_total_reviews'),

        # Average rating this user gives — shows if they are a harsh/lenient rater
        round(avg('rating'), 2).alias('user_avg_review_rating'),

        # Sentiment breakdown
        count(when(col('sentiment') == 'Positive', 1)).alias('user_positive_reviews'),
        count(when(col('sentiment') == 'Negative', 1)).alias('user_negative_reviews'),
    )
)

print(f"reviews_agg rows : {reviews_agg.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Events → Aggregate to User Level
# MAGIC - Events are at event grain (millions of rows).
# MAGIC - We aggregate to user level so one clean user-behaviour row
# MAGIC - can be joined to each order for that user.

# COMMAND ----------

# Aggregate silver_events to One Row Per user_id

events_agg = (
    spark.table(silver_events)
    .join(batch_user_ids, on='user_id', how='inner')

    .groupBy('user_id')
    .agg(
        # How many unique sessions has this user had?
        countDistinct('session_id').alias('user_total_sessions'),

        # Total events fired by this user
        count('event_id').alias('user_total_events'),

        # Average session engagement duration in seconds
        round(avg('duration_seconds'), 1).alias('user_avg_session_duration_sec'),

        # Total revenue attributed to conversion events for this user
        round(sum('conversion_value'), 2).alias('user_total_conversion_value'),

        # Preferred device (simple proxy — most used device across all sessions)
        # We use count + first-after-sort trick: count per device, take top
        # For simplicity, use first() — acceptable for a Gold analytical summary
        # In production, replace with a UDF or window rank
        count(when(col('device_type') == 'Mobile',  1)).alias('sessions_mobile'),
        count(when(col('device_type') == 'Desktop', 1)).alias('sessions_desktop'),
        count(when(col('device_type') == 'Tablet',  1)).alias('sessions_tablet'),
    )
)

print(f"events_agg rows : {events_agg.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read User Dimension
# MAGIC - Users is a small dimension table — one row per user.
# MAGIC - We read it fully (no pre-aggregation needed) and join it directly to orders.

# COMMAND ----------

# Read silver_users Dimension for Batch Users Only

users_dim = (
    spark.table(silver_users)
    .join(batch_user_ids, on='user_id', how='inner')
    .select(
        'user_id',
        'name',
        'email',
        'country',
        'city',
        'gender',
        'age',
        'membership_tier',
        'signup_date',
        'last_login',
        'total_orders',
        'lifetime_value',
        'loyalty_points',
        'referral_source',
        'preferred_category',
        'newsletter_subscribed',
    )
)

print(f"users_dim rows : {users_dim.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join All Sources to Build the Gold Table
# MAGIC - **Join order:**
# MAGIC - Start with    `orders_df`           (spine - 1 row per order)
# MAGIC - Left join     `users_dim`           (1:1 - one user per order)
# MAGIC - Left join     `items_agg`           (1:1 - pre-aggregated to order level)
# MAGIC - Left join     `products_agg`        (1:1 - pre-aggregated to order level)
# MAGIC - Left join     `reviews_agg`         (1:1 - pre-aggregated to user level)
# MAGIC - Left join     `events_agg`          (1:1 - pre-aggregated to user level)
# MAGIC - All joins are 1-to-1 at this point. No fan-out. No duplicate rows.

# COMMAND ----------

# Join All Pre-Aggregated Sources to Orders Spine

gold_df = (
    orders_df

    # ── User dimension ─────────────────────────────────────────────────────────
    # Left join keeps orders even if user record is somehow missing
    .join(users_dim,    on='user_id',  how='left')

    # ── Item-level aggregates (pre-computed to order level) ────────────────────
    .join(items_agg,    on='order_id', how='left')

    # ── Product-level aggregates (pre-computed to order level) ─────────────────
    .join(products_agg, on='order_id', how='left')

    # ── Review aggregates (at user level — safe: 1 user row per order) ─────────
    .join(reviews_agg,  on='user_id',  how='left')

    # ── Event/session aggregates (at user level — same reason) ─────────────────
    .join(events_agg,   on='user_id',  how='left')
)

print(f"Gold rows before derived columns : {gold_df.count():,}")
print(f"Should equal orders batch count  : {batch_count:,}  - must match, no fan-out")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived Business Columns

# COMMAND ----------

# Add Date Dimensions for Partitioning and Dashboard Filtering

gold_df = (
    gold_df
    .withColumn('order_date_day', to_date('order_date'))
    .withColumn('order_year', year('order_date'))
    .withColumn('order_month', month('order_date'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Net Revenue — Revenue After Discounts
# MAGIC - net_revenue = total_amount paid by customer (already includes tax + shipping - discount)
# MAGIC - We also expose the gross_discount at order level for discount reporting.

# COMMAND ----------

gold_df = (
    gold_df

    # total_amount from Silver orders is the final customer-facing amount
    .withColumn('gross_revenue',
                coalesce(col('total_amount'), lit(0.0)))

    # Net revenue strips out shipping and tax to show pure product revenue
    .withColumn('net_product_revenue',
                round(
                    coalesce(col('total_amount'), lit(0.0))
                    - coalesce(col('shipping_fee'), lit(0.0))
                    - coalesce(col('tax_amount'), lit(0.0)),
                    2
                ))

    # Total discount given on this order (from orders table — order-level coupon discount)
    .withColumn('order_discount_amount',
                coalesce(col('discount_amount'), lit(0.0)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Delivery Days 
# MAGIC - How Long Did Fulfilment Take?

# COMMAND ----------

# delivery_days = actual_delivery_date - order_date
# Null if order is not yet delivered (still in transit or cancelled).

gold_df = gold_df.withColumn(
    'delivery_days',
    when(
        col('actual_delivery').isNotNull(),
        datediff(col('actual_delivery'), to_date('order_date'))
    ).otherwise(lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Item Return Rate

# COMMAND ----------

# Returned Units / Total Units Sold
# Return rate at order level tells us how problematic this order was.
# 0 = no returns. 1.0 = everything returned.

gold_df = gold_df.withColumn(
    'item_return_rate',
    when(
        coalesce(col('total_units_sold'), lit(0)) > 0,
        round(
            coalesce(col('total_units_returned'), lit(0))
            / col('total_units_sold'),
            4
        )
    ).otherwise(lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flag High-Value Orders

# COMMAND ----------

# High value: order total >= 500.  Mid: 100–499.  Low: below 100.
# Used by finance and marketing to segment orders for reporting.

gold_df = gold_df.withColumn(
    'order_value_segment',
    when(col('gross_revenue') >= 500, lit('High'))
    .when(col('gross_revenue') >= 100, lit('Mid'))
    .otherwise(lit('Low'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Audit Columns

# COMMAND ----------

# Stamp Audit Columns on Every Row
# etl_processed_at -> when THIS Gold run touched the row
# etl_batch_id -> unique ID for this pipeline run (helps debugging)
# etl_source_table -> which Silver tables contributed to this row

gold_df = (
    gold_df
    .withColumn('etl_processed_at',  current_timestamp())
    .withColumn('etl_batch_id',      lit(ETL_BATCH_ID))
    .withColumn('etl_source_table',  lit(
        'silver_orders | silver_order_items | silver_users | '
        'silver_products | silver_reviews | silver_events'
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Write Data Quality Checks

# COMMAND ----------

print('-' * 65)
print('  Gold Order Analytics — Data Quality Report (this batch)')
print('-' * 65)

total = gold_df.count()

dq_checks = {

    # ── Primary key checks ────────────────────────────────────────────────────
    'NULL order_id (must be 0)'    : gold_df.filter(col('order_id').isNull()).count(),
    'NULL user_id'                 : gold_df.filter(col('user_id').isNull()).count(),
    'Duplicate order_id in batch'  : total - gold_df.select('order_id').distinct().count(),

    # ── Revenue checks ────────────────────────────────────────────────────────
    'Negative gross_revenue'       : gold_df.filter(col('gross_revenue') < 0).count(),
    'NULL gross_revenue'           : gold_df.filter(col('gross_revenue').isNull()).count(),
    'Negative net_product_revenue' : gold_df.filter(col('net_product_revenue') < 0).count(),

    # ── Date checks ───────────────────────────────────────────────────────────
    'NULL order_date'              : gold_df.filter(col('order_date').isNull()).count(),
    'NULL order_year'              : gold_df.filter(col('order_year').isNull()).count(),

    # ── Item metric checks ────────────────────────────────────────────────────
    'NULL total_line_items'        : gold_df.filter(col('total_line_items').isNull()).count(),
    'Return rate > 1.0'            : gold_df.filter(col('item_return_rate') > 1.0).count(),

    # ── Dimension checks ──────────────────────────────────────────────────────
    'NULL country'                 : gold_df.filter(col('country').isNull()).count(),
    'NULL membership_tier'         : gold_df.filter(col('membership_tier').isNull()).count(),
}

all_passed = True
critical_checks = {
    'NULL order_id (must be 0)',
    'Duplicate order_id in batch',
    'Negative gross_revenue',
    'NULL order_date',
}

for label, fail_count in dq_checks.items():
    pct    = (fail_count / total * 100) if total else 0
    is_critical = label in critical_checks
    status = '✅' if fail_count == 0 else ('❌' if is_critical else '⚠️ ')
    if fail_count > 0 and is_critical:
        all_passed = False
    print(f'  {status}  {label:<40} {fail_count:>8,}  ({pct:5.2f}%)')

print('-' * 65)
print(f"  Total rows in this batch : {total:,}")
print(f"  DQ result : {'✅ PASSED' if all_passed else '❌ CRITICAL FAILURES — INVESTIGATE BEFORE PROCEEDING'}")
print('-' * 65)

# Stop the notebook if critical checks failed — do not write bad data to Gold
if not all_passed:
    raise Exception("Critical DQ checks failed. Gold table was NOT updated. See report above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold — Delta MERGE (Upsert)
# MAGIC - **Why MERGE and not overwrite?**
# MAGIC - MERGE updates existing rows (corrections from Silver propagate correctly)
# MAGIC - MERGE inserts new rows (new orders added since last run)
# MAGIC - Running this notebook multiple times produces the same Gold table — idempotent
# MAGIC - No data is lost from previous runs
# MAGIC - **WHEN MATCHED → UPDATE ALL**
# MAGIC - A Silver correction was re-processed and now has a newer `etl_processed_at`.
# MAGIC - The MERGE overwrites the stale Gold row with the corrected values.
# MAGIC - **WHEN NOT MATCHED → INSERT ALL**
# MAGIC - A brand-new order that has never been in Gold before is inserted.

# COMMAND ----------

# Delta MERGE — Upsert Batch into Gold Table

if spark.catalog.tableExists(gold_table):

    # Table already exists — MERGE to handle both new and updated rows
    gold_delta = DeltaTable.forName(spark, gold_table)

    (
        gold_delta.alias('gold')
        .merge(
            gold_df.alias('new'),
            f'gold.{MERGE_KEY} = new.{MERGE_KEY}'   # match on order_id
        )
        .whenMatchedUpdateAll()    # correction: overwrite stale Gold row
        .whenNotMatchedInsertAll() # new order: insert fresh row
        .execute()
    )

    print(f"✅  MERGE complete → {gold_table}")

else:

    # First run — table does not exist yet.
    # Write with partitioning so all future queries benefit from partition pruning.
    (
        gold_df.write
            .format('delta')
            .mode('append')
            .option('mergeSchema', 'true')
            # Partition by year + month so "last 30 days" queries
            # scan at most 2 partitions instead of the full table.
            .partitionBy('order_year', 'order_month')
            .saveAsTable(gold_table)
    )

    print(f"✅  Gold table created and written → {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize the Gold Table
# MAGIC - **OPTIMIZE** compacts the many small Delta files written by incremental
# MAGIC - MERGE operations into larger, more efficient files.
# MAGIC - This dramatically speeds up dashboard queries.
# MAGIC - **Z-ORDER BY** physically co-locates rows with the same values for the
# MAGIC - chosen columns in the same files. Queries that filter on these columns
# MAGIC - (e.g. `WHERE country = 'United States'`) skip files that don't match,
# MAGIC - making scans much faster.
# MAGIC - **Z-ORDER** column choices:
# MAGIC - `user_id`         → user-level drilldowns in dashboards
# MAGIC - `status`          → order status filtering (completed, cancelled, etc.)
# MAGIC - `country`         → geography-based revenue reports
# MAGIC - `membership_tier` → loyalty tier analysis

# COMMAND ----------

# OPTIMIZE + Z-ORDER for Fast Dashboard Query Performance

spark.sql(f"""
    OPTIMIZE {gold_table}
    ZORDER BY (user_id, status, country, membership_tier)
""")

print(f"✅  OPTIMIZE + ZORDER complete on {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Write Business Validation

# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue Summary by Year and Month

# COMMAND ----------

# Key business report: how much revenue was generated each month?

spark.sql(f"""
    SELECT
        order_year,
        order_month,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(gross_revenue), 2) AS total_revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value,
        ROUND(SUM(order_discount_amount), 2) AS total_discounts,
        SUM(total_units_sold) AS total_units_sold,
        SUM(total_units_returned) AS total_units_returned
    FROM  {gold_table}
    GROUP BY order_year, order_month
    ORDER BY order_year DESC, order_month DESC
    LIMIT 12
""").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue and Orders by Country

# COMMAND ----------

# Geography report: which countries drive the most revenue?

spark.sql(f"""
    SELECT
        country,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        ROUND(SUM(gross_revenue),2) AS total_revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value,
        ROUND(AVG(delivery_days), 1) AS avg_delivery_days
    FROM  {gold_table}
    WHERE country IS NOT NULL
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Order Status Distribution

# COMMAND ----------

# Operations report: how are orders distributed across statuses?

spark.sql(f"""
    SELECT
        status,
        COUNT(*) AS order_count,
        ROUND(SUM(gross_revenue), 2) AS revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value,
        SUM(total_units_returned) AS units_returned
    FROM  {gold_table}
    GROUP BY status
    ORDER BY order_count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Membership Tier

# COMMAND ----------

# Loyalty report: how much revenue comes from each tier?

spark.sql(f"""
    SELECT
        membership_tier,
        COUNT(DISTINCT user_id) AS customers,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(gross_revenue), 2) AS total_revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value,
        ROUND(AVG(user_avg_review_rating), 2) AS avg_review_rating
    FROM  {gold_table}
    WHERE membership_tier IS NOT NULL
    GROUP BY membership_tier
    ORDER BY total_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Channel and Payment Method Mix
# MAGIC

# COMMAND ----------

# Marketing report: which channel and payment method perform best?

spark.sql(f"""
    SELECT
        channel,
        payment_method,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(gross_revenue), 2) AS revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value
    FROM  {gold_table}
    GROUP BY channel, payment_method
    ORDER BY revenue DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Gold Table Row Count Confirmation

# COMMAND ----------

final_count = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {gold_table}").collect()[0]['total_rows']
print(f"✅  Gold table {gold_table} contains {final_count:,} rows total.")
print(f"    Batch written in this run    : {batch_count:,} rows (via MERGE)")
print(f"    ETL batch ID                 : {ETL_BATCH_ID}")