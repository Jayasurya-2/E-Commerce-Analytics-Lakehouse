# Databricks notebook source
# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

gold_schema = 'e_commerce.gold'
gold_table = f'{gold_schema}.gold_order_analytics'

# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue Summary by Year and Month

# COMMAND ----------

# Key business report: how much revenue was generated each month?

revenue_yr_mnth_df = spark.sql(f"""
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
""")

display(revenue_yr_mnth_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue and Orders by Country

# COMMAND ----------

# Geography report: which countries drive the most revenue?

revenue_country_df = spark.sql(f"""
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
""")

display(revenue_country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Status Distribution

# COMMAND ----------

# Operations report: how are orders distributed across statuses?

order_distribution_df = spark.sql(f"""
    SELECT
        status,
        COUNT(*) AS order_count,
        ROUND(SUM(gross_revenue), 2) AS revenue,
        ROUND(AVG(gross_revenue), 2) AS avg_order_value,
        SUM(total_units_returned) AS units_returned
    FROM  {gold_table}
    GROUP BY status
    ORDER BY order_count DESC
""")

display(order_distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Membership Tier

# COMMAND ----------

# Loyalty report: how much revenue comes from each tier?

revenue_member_df = spark.sql(f"""
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
""")

display(revenue_member_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Channel and Payment Method Mix

# COMMAND ----------

# Marketing report: which channel and payment method perform best?

channel_payment_df = spark.sql(f"""
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
""")

display(channel_payment_df)