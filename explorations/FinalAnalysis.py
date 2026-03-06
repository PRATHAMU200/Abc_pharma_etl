# Databricks notebook source
# MAGIC %md
# MAGIC ### This is the notebook showing all results for the questions:
# MAGIC
# MAGIC **Note**: This notebook is not executed as part of the pipeline.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("USE CATALOG `abccompany`")
spark.sql("USE SCHEMA `fixeddata`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Calculate the Market share of the products  
# MAGIC General
# MAGIC Be specific to ABC pharma

# COMMAND ----------

display(
    spark.table("g_market_share")
)

# COMMAND ----------

display(
    spark.table("g_market_share")
         .filter(col("prod_mnf") == "ABC_PHARMA")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Find out the Sales trend of the Products (at Brand level) for last 4 quarters
# MAGIC

# COMMAND ----------

display(
    spark.table("g_brand_sales_trend_qtr")
         .orderBy("brand_name", "quarter")
)

# COMMAND ----------

display(
    spark.table("g_brand_sales_trend_qtr")
         .filter(col("prod_mnf") == "ABC_PHARMA")
         .orderBy("brand_name", "quarter")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate the sales of a product by specialty type 
# MAGIC

# COMMAND ----------

display(
    spark.table("g_product_sales_by_specialty")
         .orderBy(desc("total_trx_unit"))
)

# COMMAND ----------

display(
    spark.table("g_product_sales_by_specialty")
         .filter(col("prod_mnf") == "ABC_PHARMA")
         .orderBy(desc("total_trx_unit"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Find out the Top 10 districts based on the Sales from last year
# MAGIC

# COMMAND ----------

display(
    spark.table("g_top_10_districts_by_sales")
)

# COMMAND ----------

display(
    spark.table("g_top_10_districts_by_sales")
         .filter(col("prod_mnf") == "ABC_PHARMA")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top 5 best performing physicians in each City (in 2023) based on sales
# MAGIC

# COMMAND ----------

display(
    spark.table("g_top5_physicians_by_city")
         .orderBy("city", "state", "rank_in_city")
)
